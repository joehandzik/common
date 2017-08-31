// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expfmt

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"mime"
	"net/http"
	"strconv"

	dto "github.com/prometheus/client_model/go"
	"github.com/golang/protobuf/proto"
	"github.com/matttproud/golang_protobuf_extensions/pbutil"
	"github.com/prometheus/common/model"
)

type Metric struct {
       Labels map[string]string `json:"labels,omitempty"`
       Value string `json:"value"`
}

type Summary struct {
       Labels map[string]string `json:"labels,omitempty"`
       Quantiles map[string]string `json:"quantiles,omitempty"`
       Count string `json:"count"`
       Sum string `json:"sum"`
}

type Histogram struct {
       Labels map[string]string `json:"labels,omitempty"`
       Buckets map[string]string `json:"buckets,omitempty"`
       Count string `json:"count"`
       Sum string `json:"sum"`
}

type Family struct {
       Name    string        `json:"name"`
       Help    string        `json:"help"`
       Type    string        `json:"type"`
       Metrics []interface{} `json:"metrics,omitempty"`
}

type iloResp struct {
       ODataType string           `json:"@odata.type"`
       Port      string           `json:"port"`
       Data      []Family    `json:"data"`
}

// Decoder types decode an input stream into metric families.
type Decoder interface {
	Decode(*dto.MetricFamily) error
}

// DecodeOptions contains options used by the Decoder and in sample extraction.
type DecodeOptions struct {
	// Timestamp is added to each value from the stream that has no explicit timestamp set.
	Timestamp model.Time
}

// ResponseFormat extracts the correct format from a HTTP response header.
// If no matching format can be found FormatUnknown is returned.
func ResponseFormat(h http.Header) Format {
	ct := h.Get(hdrContentType)

	mediatype, params, err := mime.ParseMediaType(ct)
	if err != nil {
		return FmtUnknown
	}

	const textType = "text/plain"
	const jsonType = "application/json"

	switch mediatype {
	case ProtoType:
		if p, ok := params["proto"]; ok && p != ProtoProtocol {
			return FmtUnknown
		}
		if e, ok := params["encoding"]; ok && e != "delimited" {
			return FmtUnknown
		}
		return FmtProtoDelim

	case textType:
		if v, ok := params["version"]; ok && v != TextVersion {
			return FmtUnknown
		}
		return FmtText

	case jsonType:
		return FmtJson
	}

	return FmtUnknown
}

// NewDecoder returns a new decoder based on the given input format.
// If the input format does not imply otherwise, a text format decoder is returned.
func NewDecoder(r io.Reader, format Format) Decoder {
	switch format {
	case FmtProtoDelim:
		return &protoDecoder{r: r}
	case FmtJson:
		return &jsonDecoder{r: r}
	}
	return &textDecoder{r: r}
}

// jsonDecoder implements the Decoder interface for JSON from iLO.
type jsonDecoder struct {
	r io.Reader
	fams []*dto.MetricFamily
}

func buildLabelPairs(labels map[string]string) []*dto.LabelPair {
	var labelPairs []*dto.LabelPair
	for key, value := range labels {
		newLabelPair := &dto.LabelPair{Name: proto.String(key), Value: proto.String(value)}
		labelPairs = append(labelPairs, newLabelPair)
	}
	return labelPairs
}

func buildBuckets(bucketMap map[string]string) []*dto.Bucket {
	var buckets []*dto.Bucket
	for key, value := range bucketMap {
		upperBound, _ := strconv.ParseFloat(key, 64)
		cumulativeCount, _ := strconv.ParseUint(value, 10, 64)
		newBucket := &dto.Bucket{UpperBound: proto.Float64(upperBound), CumulativeCount: proto.Uint64(cumulativeCount)}
		buckets = append(buckets, newBucket)
	}
	return buckets
}

func buildQuantiles(quantileMap map[string]string) []*dto.Quantile {
	var quantiles []*dto.Quantile
	for key, value := range quantileMap {
		quantile, _ := strconv.ParseFloat(key, 64)
		newValue, _ := strconv.ParseFloat(value, 64)
		newQuantile := &dto.Quantile{Quantile: proto.Float64(quantile), Value: proto.Float64(newValue)}
		quantiles = append(quantiles, newQuantile)
	}
	return quantiles
}

func (d *jsonDecoder) JsonToMetricFamilies(in io.Reader) (map[string]*dto.MetricFamily, error) {
	tmpBody, _ := ioutil.ReadAll(d.r)
	var transResp iloResp
	if err := json.Unmarshal(tmpBody, &transResp); err != nil {
		return nil, err
	}
	newMetricFamilyCollection := map[string]*dto.MetricFamily{}
	for _, metricFamily := range transResp.Data {
		if (metricFamily.Type == "COUNTER") {
			newMetricFamilyCollection[metricFamily.Name] = &dto.MetricFamily{Name: proto.String(metricFamily.Name), Help: proto.String(metricFamily.Help), Type: dto.MetricType_COUNTER.Enum()}
			for _, metric := range metricFamily.Metrics {
				jsonToMetric := metric.(Metric)
				value, _ := strconv.ParseFloat(jsonToMetric.Value, 64)
				newCounter := &dto.Counter{Value: proto.Float64(value)}
				newLabelPair := buildLabelPairs(jsonToMetric.Labels)
				newMetric := &dto.Metric{Label: newLabelPair, Counter: newCounter}
				newMetricFamilyCollection[metricFamily.Name].Metric = append(newMetricFamilyCollection[metricFamily.Name].Metric, newMetric)
			}
		} else if (metricFamily.Type == "GAUGE") {
			newMetricFamilyCollection[metricFamily.Name] = &dto.MetricFamily{Name: proto.String(metricFamily.Name), Help: proto.String(metricFamily.Help), Type: dto.MetricType_GAUGE.Enum()}
			for _, metric := range metricFamily.Metrics {
				jsonToMetric := metric.(Metric)
				value, _ := strconv.ParseFloat(jsonToMetric.Value, 64)
				newGauge := &dto.Gauge{Value: proto.Float64(value)}
				newLabelPair := buildLabelPairs(jsonToMetric.Labels)
				newMetric := &dto.Metric{Label: newLabelPair, Gauge: newGauge}
				newMetricFamilyCollection[metricFamily.Name].Metric = append(newMetricFamilyCollection[metricFamily.Name].Metric, newMetric)
			}
		} else if (metricFamily.Type == "UNTYPED") {
			newMetricFamilyCollection[metricFamily.Name] = &dto.MetricFamily{Name: proto.String(metricFamily.Name), Help: proto.String(metricFamily.Help), Type: dto.MetricType_UNTYPED.Enum()}
			for _, metric := range metricFamily.Metrics {
				jsonToMetric := metric.(Metric)
				value, _ := strconv.ParseFloat(jsonToMetric.Value, 64)
				newUntyped := &dto.Untyped{Value: proto.Float64(value)}
				newLabelPair := buildLabelPairs(jsonToMetric.Labels)
				newMetric := &dto.Metric{Label: newLabelPair, Untyped: newUntyped}
				newMetricFamilyCollection[metricFamily.Name].Metric = append(newMetricFamilyCollection[metricFamily.Name].Metric, newMetric)
			}
		} else if (metricFamily.Type == "SUMMARY") {
			newMetricFamilyCollection[metricFamily.Name] = &dto.MetricFamily{Name: proto.String(metricFamily.Name), Help: proto.String(metricFamily.Help), Type: dto.MetricType_SUMMARY.Enum()}
			for _, metric := range metricFamily.Metrics {
				jsonToMetric := metric.(Summary)
				sampleCount, _ := strconv.ParseUint(jsonToMetric.Count, 10, 64)
				sampleSum, _ := strconv.ParseFloat(jsonToMetric.Sum, 64)
				newQuantiles := buildQuantiles(jsonToMetric.Quantiles)
				newSummary := &dto.Summary{SampleCount: proto.Uint64(sampleCount), SampleSum: proto.Float64(sampleSum), Quantile: newQuantiles}
				newLabelPair := buildLabelPairs(jsonToMetric.Labels)
				newMetric := &dto.Metric{Label: newLabelPair, Summary: newSummary}
				newMetricFamilyCollection[metricFamily.Name].Metric = append(newMetricFamilyCollection[metricFamily.Name].Metric, newMetric)
			}
		} else if (metricFamily.Type == "HISTOGRAM") {
			newMetricFamilyCollection[metricFamily.Name] = &dto.MetricFamily{Name: proto.String(metricFamily.Name), Help: proto.String(metricFamily.Help), Type: dto.MetricType_HISTOGRAM.Enum()}
			for _, metric := range metricFamily.Metrics {
				jsonToMetric := metric.(Histogram)
				sampleCount, _ := strconv.ParseUint(jsonToMetric.Count, 10, 64)
				sampleSum, _ := strconv.ParseFloat(jsonToMetric.Sum, 64)
				newBuckets := buildBuckets(jsonToMetric.Buckets)
				newHistogram := &dto.Histogram{SampleCount: proto.Uint64(sampleCount), SampleSum: proto.Float64(sampleSum), Bucket: newBuckets}
				newLabelPair := buildLabelPairs(jsonToMetric.Labels)
				newMetric := &dto.Metric{Label: newLabelPair, Histogram: newHistogram}
				newMetricFamilyCollection[metricFamily.Name].Metric = append(newMetricFamilyCollection[metricFamily.Name].Metric, newMetric)
			}
		} else {
			return nil, nil
		}
	}

	return newMetricFamilyCollection, nil
}

func (d *jsonDecoder) Decode(v *dto.MetricFamily) error {
	if len(d.fams) == 0 {
		fams, err := d.JsonToMetricFamilies(d.r)
		if err != nil {
			return err
		}
		if len(fams) == 0 {
			return io.EOF
		}
		d.fams = make([]*dto.MetricFamily, 0, len(fams))
		for _, f := range fams {
			d.fams = append(d.fams, f)
		}
	}
	*v = *d.fams[0]
	d.fams = d.fams[1:]
	return nil
}

// protoDecoder implements the Decoder interface for protocol buffers.
type protoDecoder struct {
	r io.Reader
}

// Decode implements the Decoder interface.
func (d *protoDecoder) Decode(v *dto.MetricFamily) error {
	_, err := pbutil.ReadDelimited(d.r, v)
	if err != nil {
		return err
	}
	if !model.IsValidMetricName(model.LabelValue(v.GetName())) {
		return fmt.Errorf("invalid metric name %q", v.GetName())
	}
	for _, m := range v.GetMetric() {
		if m == nil {
			continue
		}
		for _, l := range m.GetLabel() {
			if l == nil {
				continue
			}
			if !model.LabelValue(l.GetValue()).IsValid() {
				return fmt.Errorf("invalid label value %q", l.GetValue())
			}
			if !model.LabelName(l.GetName()).IsValid() {
				return fmt.Errorf("invalid label name %q", l.GetName())
			}
		}
	}
	return nil
}

// textDecoder implements the Decoder interface for the text protocol.
type textDecoder struct {
	r    io.Reader
	p    TextParser
	fams []*dto.MetricFamily
}

// Decode implements the Decoder interface.
func (d *textDecoder) Decode(v *dto.MetricFamily) error {
	// TODO(fabxc): Wrap this as a line reader to make streaming safer.
	if len(d.fams) == 0 {
		// No cached metric families, read everything and parse metrics.
		fams, err := d.p.TextToMetricFamilies(d.r)
		if err != nil {
			return err
		}
		if len(fams) == 0 {
			return io.EOF
		}
		d.fams = make([]*dto.MetricFamily, 0, len(fams))
		for _, f := range fams {
			d.fams = append(d.fams, f)
		}
	}

	*v = *d.fams[0]
	d.fams = d.fams[1:]

	return nil
}

// SampleDecoder wraps a Decoder to extract samples from the metric families
// decoded by the wrapped Decoder.
type SampleDecoder struct {
	Dec  Decoder
	Opts *DecodeOptions

	f dto.MetricFamily
}

// Decode calls the Decode method of the wrapped Decoder and then extracts the
// samples from the decoded MetricFamily into the provided model.Vector.
func (sd *SampleDecoder) Decode(s *model.Vector) error {
	err := sd.Dec.Decode(&sd.f)
	if err != nil {
		return err
	}
	*s, err = extractSamples(&sd.f, sd.Opts)
	return err
}

// ExtractSamples builds a slice of samples from the provided metric
// families. If an error occurs during sample extraction, it continues to
// extract from the remaining metric families. The returned error is the last
// error that has occured.
func ExtractSamples(o *DecodeOptions, fams ...*dto.MetricFamily) (model.Vector, error) {
	var (
		all     model.Vector
		lastErr error
	)
	for _, f := range fams {
		some, err := extractSamples(f, o)
		if err != nil {
			lastErr = err
			continue
		}
		all = append(all, some...)
	}
	return all, lastErr
}

func extractSamples(f *dto.MetricFamily, o *DecodeOptions) (model.Vector, error) {
	switch f.GetType() {
	case dto.MetricType_COUNTER:
		return extractCounter(o, f), nil
	case dto.MetricType_GAUGE:
		return extractGauge(o, f), nil
	case dto.MetricType_SUMMARY:
		return extractSummary(o, f), nil
	case dto.MetricType_UNTYPED:
		return extractUntyped(o, f), nil
	case dto.MetricType_HISTOGRAM:
		return extractHistogram(o, f), nil
	}
	return nil, fmt.Errorf("expfmt.extractSamples: unknown metric family type %v", f.GetType())
}

func extractCounter(o *DecodeOptions, f *dto.MetricFamily) model.Vector {
	samples := make(model.Vector, 0, len(f.Metric))

	for _, m := range f.Metric {
		if m.Counter == nil {
			continue
		}

		lset := make(model.LabelSet, len(m.Label)+1)
		for _, p := range m.Label {
			lset[model.LabelName(p.GetName())] = model.LabelValue(p.GetValue())
		}
		lset[model.MetricNameLabel] = model.LabelValue(f.GetName())

		smpl := &model.Sample{
			Metric: model.Metric(lset),
			Value:  model.SampleValue(m.Counter.GetValue()),
		}

		if m.TimestampMs != nil {
			smpl.Timestamp = model.TimeFromUnixNano(*m.TimestampMs * 1000000)
		} else {
			smpl.Timestamp = o.Timestamp
		}

		samples = append(samples, smpl)
	}

	return samples
}

func extractGauge(o *DecodeOptions, f *dto.MetricFamily) model.Vector {
	samples := make(model.Vector, 0, len(f.Metric))

	for _, m := range f.Metric {
		if m.Gauge == nil {
			continue
		}

		lset := make(model.LabelSet, len(m.Label)+1)
		for _, p := range m.Label {
			lset[model.LabelName(p.GetName())] = model.LabelValue(p.GetValue())
		}
		lset[model.MetricNameLabel] = model.LabelValue(f.GetName())

		smpl := &model.Sample{
			Metric: model.Metric(lset),
			Value:  model.SampleValue(m.Gauge.GetValue()),
		}

		if m.TimestampMs != nil {
			smpl.Timestamp = model.TimeFromUnixNano(*m.TimestampMs * 1000000)
		} else {
			smpl.Timestamp = o.Timestamp
		}

		samples = append(samples, smpl)
	}

	return samples
}

func extractUntyped(o *DecodeOptions, f *dto.MetricFamily) model.Vector {
	samples := make(model.Vector, 0, len(f.Metric))

	for _, m := range f.Metric {
		if m.Untyped == nil {
			continue
		}

		lset := make(model.LabelSet, len(m.Label)+1)
		for _, p := range m.Label {
			lset[model.LabelName(p.GetName())] = model.LabelValue(p.GetValue())
		}
		lset[model.MetricNameLabel] = model.LabelValue(f.GetName())

		smpl := &model.Sample{
			Metric: model.Metric(lset),
			Value:  model.SampleValue(m.Untyped.GetValue()),
		}

		if m.TimestampMs != nil {
			smpl.Timestamp = model.TimeFromUnixNano(*m.TimestampMs * 1000000)
		} else {
			smpl.Timestamp = o.Timestamp
		}

		samples = append(samples, smpl)
	}

	return samples
}

func extractSummary(o *DecodeOptions, f *dto.MetricFamily) model.Vector {
	samples := make(model.Vector, 0, len(f.Metric))

	for _, m := range f.Metric {
		if m.Summary == nil {
			continue
		}

		timestamp := o.Timestamp
		if m.TimestampMs != nil {
			timestamp = model.TimeFromUnixNano(*m.TimestampMs * 1000000)
		}

		for _, q := range m.Summary.Quantile {
			lset := make(model.LabelSet, len(m.Label)+2)
			for _, p := range m.Label {
				lset[model.LabelName(p.GetName())] = model.LabelValue(p.GetValue())
			}
			// BUG(matt): Update other names to "quantile".
			lset[model.LabelName(model.QuantileLabel)] = model.LabelValue(fmt.Sprint(q.GetQuantile()))
			lset[model.MetricNameLabel] = model.LabelValue(f.GetName())

			samples = append(samples, &model.Sample{
				Metric:    model.Metric(lset),
				Value:     model.SampleValue(q.GetValue()),
				Timestamp: timestamp,
			})
		}

		lset := make(model.LabelSet, len(m.Label)+1)
		for _, p := range m.Label {
			lset[model.LabelName(p.GetName())] = model.LabelValue(p.GetValue())
		}
		lset[model.MetricNameLabel] = model.LabelValue(f.GetName() + "_sum")

		samples = append(samples, &model.Sample{
			Metric:    model.Metric(lset),
			Value:     model.SampleValue(m.Summary.GetSampleSum()),
			Timestamp: timestamp,
		})

		lset = make(model.LabelSet, len(m.Label)+1)
		for _, p := range m.Label {
			lset[model.LabelName(p.GetName())] = model.LabelValue(p.GetValue())
		}
		lset[model.MetricNameLabel] = model.LabelValue(f.GetName() + "_count")

		samples = append(samples, &model.Sample{
			Metric:    model.Metric(lset),
			Value:     model.SampleValue(m.Summary.GetSampleCount()),
			Timestamp: timestamp,
		})
	}

	return samples
}

func extractHistogram(o *DecodeOptions, f *dto.MetricFamily) model.Vector {
	samples := make(model.Vector, 0, len(f.Metric))

	for _, m := range f.Metric {
		if m.Histogram == nil {
			continue
		}

		timestamp := o.Timestamp
		if m.TimestampMs != nil {
			timestamp = model.TimeFromUnixNano(*m.TimestampMs * 1000000)
		}

		infSeen := false

		for _, q := range m.Histogram.Bucket {
			lset := make(model.LabelSet, len(m.Label)+2)
			for _, p := range m.Label {
				lset[model.LabelName(p.GetName())] = model.LabelValue(p.GetValue())
			}
			lset[model.LabelName(model.BucketLabel)] = model.LabelValue(fmt.Sprint(q.GetUpperBound()))
			lset[model.MetricNameLabel] = model.LabelValue(f.GetName() + "_bucket")

			if math.IsInf(q.GetUpperBound(), +1) {
				infSeen = true
			}

			samples = append(samples, &model.Sample{
				Metric:    model.Metric(lset),
				Value:     model.SampleValue(q.GetCumulativeCount()),
				Timestamp: timestamp,
			})
		}

		lset := make(model.LabelSet, len(m.Label)+1)
		for _, p := range m.Label {
			lset[model.LabelName(p.GetName())] = model.LabelValue(p.GetValue())
		}
		lset[model.MetricNameLabel] = model.LabelValue(f.GetName() + "_sum")

		samples = append(samples, &model.Sample{
			Metric:    model.Metric(lset),
			Value:     model.SampleValue(m.Histogram.GetSampleSum()),
			Timestamp: timestamp,
		})

		lset = make(model.LabelSet, len(m.Label)+1)
		for _, p := range m.Label {
			lset[model.LabelName(p.GetName())] = model.LabelValue(p.GetValue())
		}
		lset[model.MetricNameLabel] = model.LabelValue(f.GetName() + "_count")

		count := &model.Sample{
			Metric:    model.Metric(lset),
			Value:     model.SampleValue(m.Histogram.GetSampleCount()),
			Timestamp: timestamp,
		}
		samples = append(samples, count)

		if !infSeen {
			// Append an infinity bucket sample.
			lset := make(model.LabelSet, len(m.Label)+2)
			for _, p := range m.Label {
				lset[model.LabelName(p.GetName())] = model.LabelValue(p.GetValue())
			}
			lset[model.LabelName(model.BucketLabel)] = model.LabelValue("+Inf")
			lset[model.MetricNameLabel] = model.LabelValue(f.GetName() + "_bucket")

			samples = append(samples, &model.Sample{
				Metric:    model.Metric(lset),
				Value:     count.Value,
				Timestamp: timestamp,
			})
		}
	}

	return samples
}
