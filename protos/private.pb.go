// Code generated by protoc-gen-gogo.
// source: private.proto
// DO NOT EDIT!

/*
Package protos is a generated protocol buffer package.

It is generated from these files:
	private.proto

It has these top-level messages:
	ExecTaskParams
	ExecTaskResult
	ExecSuperstepParams
	ExecSuperstepResult
	Aggregator
	JobCheckpoint
*/
package protos

import proto "github.com/gogo/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion2 // please upgrade the proto package

type ExecTaskParams struct {
	TaskId          int32                `protobuf:"varint,1,opt,name=taskId,proto3" json:"taskId,omitempty"`
	JobId           string               `protobuf:"bytes,2,opt,name=jobId,proto3" json:"jobId,omitempty"`
	Store           string               `protobuf:"bytes,3,opt,name=store,proto3" json:"store,omitempty"`
	StoreParams     []byte               `protobuf:"bytes,4,opt,name=storeParams,proto3" json:"storeParams,omitempty"`
	Algorithm       string               `protobuf:"bytes,5,opt,name=algorithm,proto3" json:"algorithm,omitempty"`
	AlgorithmParams []byte               `protobuf:"bytes,6,opt,name=algorithmParams,proto3" json:"algorithmParams,omitempty"`
	SuperstepParams *ExecSuperstepParams `protobuf:"bytes,7,opt,name=superstepParams" json:"superstepParams,omitempty"`
}

func (m *ExecTaskParams) Reset()                    { *m = ExecTaskParams{} }
func (m *ExecTaskParams) String() string            { return proto.CompactTextString(m) }
func (*ExecTaskParams) ProtoMessage()               {}
func (*ExecTaskParams) Descriptor() ([]byte, []int) { return fileDescriptorPrivate, []int{0} }

func (m *ExecTaskParams) GetTaskId() int32 {
	if m != nil {
		return m.TaskId
	}
	return 0
}

func (m *ExecTaskParams) GetJobId() string {
	if m != nil {
		return m.JobId
	}
	return ""
}

func (m *ExecTaskParams) GetStore() string {
	if m != nil {
		return m.Store
	}
	return ""
}

func (m *ExecTaskParams) GetStoreParams() []byte {
	if m != nil {
		return m.StoreParams
	}
	return nil
}

func (m *ExecTaskParams) GetAlgorithm() string {
	if m != nil {
		return m.Algorithm
	}
	return ""
}

func (m *ExecTaskParams) GetAlgorithmParams() []byte {
	if m != nil {
		return m.AlgorithmParams
	}
	return nil
}

func (m *ExecTaskParams) GetSuperstepParams() *ExecSuperstepParams {
	if m != nil {
		return m.SuperstepParams
	}
	return nil
}

type ExecTaskResult struct {
	TaskId          int32                `protobuf:"varint,1,opt,name=taskId,proto3" json:"taskId,omitempty"`
	JobId           string               `protobuf:"bytes,2,opt,name=jobId,proto3" json:"jobId,omitempty"`
	SuperstepResult *ExecSuperstepResult `protobuf:"bytes,3,opt,name=superstepResult" json:"superstepResult,omitempty"`
}

func (m *ExecTaskResult) Reset()                    { *m = ExecTaskResult{} }
func (m *ExecTaskResult) String() string            { return proto.CompactTextString(m) }
func (*ExecTaskResult) ProtoMessage()               {}
func (*ExecTaskResult) Descriptor() ([]byte, []int) { return fileDescriptorPrivate, []int{1} }

func (m *ExecTaskResult) GetTaskId() int32 {
	if m != nil {
		return m.TaskId
	}
	return 0
}

func (m *ExecTaskResult) GetJobId() string {
	if m != nil {
		return m.JobId
	}
	return ""
}

func (m *ExecTaskResult) GetSuperstepResult() *ExecSuperstepResult {
	if m != nil {
		return m.SuperstepResult
	}
	return nil
}

type ExecSuperstepParams struct {
	Superstep    int32         `protobuf:"varint,1,opt,name=superstep,proto3" json:"superstep,omitempty"`
	VertexRanges [][]byte      `protobuf:"bytes,2,rep,name=vertexRanges" json:"vertexRanges,omitempty"`
	Aggregators  []*Aggregator `protobuf:"bytes,3,rep,name=aggregators" json:"aggregators,omitempty"`
}

func (m *ExecSuperstepParams) Reset()                    { *m = ExecSuperstepParams{} }
func (m *ExecSuperstepParams) String() string            { return proto.CompactTextString(m) }
func (*ExecSuperstepParams) ProtoMessage()               {}
func (*ExecSuperstepParams) Descriptor() ([]byte, []int) { return fileDescriptorPrivate, []int{2} }

func (m *ExecSuperstepParams) GetSuperstep() int32 {
	if m != nil {
		return m.Superstep
	}
	return 0
}

func (m *ExecSuperstepParams) GetVertexRanges() [][]byte {
	if m != nil {
		return m.VertexRanges
	}
	return nil
}

func (m *ExecSuperstepParams) GetAggregators() []*Aggregator {
	if m != nil {
		return m.Aggregators
	}
	return nil
}

type ExecSuperstepResult struct {
	Aggregators []*Aggregator              `protobuf:"bytes,1,rep,name=aggregators" json:"aggregators,omitempty"`
	Stats       *ExecSuperstepResult_Stats `protobuf:"bytes,2,opt,name=stats" json:"stats,omitempty"`
}

func (m *ExecSuperstepResult) Reset()                    { *m = ExecSuperstepResult{} }
func (m *ExecSuperstepResult) String() string            { return proto.CompactTextString(m) }
func (*ExecSuperstepResult) ProtoMessage()               {}
func (*ExecSuperstepResult) Descriptor() ([]byte, []int) { return fileDescriptorPrivate, []int{3} }

func (m *ExecSuperstepResult) GetAggregators() []*Aggregator {
	if m != nil {
		return m.Aggregators
	}
	return nil
}

func (m *ExecSuperstepResult) GetStats() *ExecSuperstepResult_Stats {
	if m != nil {
		return m.Stats
	}
	return nil
}

type ExecSuperstepResult_Stats struct {
	TotalDuration     int32 `protobuf:"varint,1,opt,name=totalDuration,proto3" json:"totalDuration,omitempty"`
	ComputedCount     int32 `protobuf:"varint,2,opt,name=computedCount,proto3" json:"computedCount,omitempty"`
	ComputeDuration   int32 `protobuf:"varint,3,opt,name=computeDuration,proto3" json:"computeDuration,omitempty"`
	SentMessagesCount int32 `protobuf:"varint,4,opt,name=sentMessagesCount,proto3" json:"sentMessagesCount,omitempty"`
	HaltedCount       int32 `protobuf:"varint,5,opt,name=haltedCount,proto3" json:"haltedCount,omitempty"`
	InactiveCount     int32 `protobuf:"varint,6,opt,name=inactiveCount,proto3" json:"inactiveCount,omitempty"`
}

func (m *ExecSuperstepResult_Stats) Reset()         { *m = ExecSuperstepResult_Stats{} }
func (m *ExecSuperstepResult_Stats) String() string { return proto.CompactTextString(m) }
func (*ExecSuperstepResult_Stats) ProtoMessage()    {}
func (*ExecSuperstepResult_Stats) Descriptor() ([]byte, []int) {
	return fileDescriptorPrivate, []int{3, 0}
}

func (m *ExecSuperstepResult_Stats) GetTotalDuration() int32 {
	if m != nil {
		return m.TotalDuration
	}
	return 0
}

func (m *ExecSuperstepResult_Stats) GetComputedCount() int32 {
	if m != nil {
		return m.ComputedCount
	}
	return 0
}

func (m *ExecSuperstepResult_Stats) GetComputeDuration() int32 {
	if m != nil {
		return m.ComputeDuration
	}
	return 0
}

func (m *ExecSuperstepResult_Stats) GetSentMessagesCount() int32 {
	if m != nil {
		return m.SentMessagesCount
	}
	return 0
}

func (m *ExecSuperstepResult_Stats) GetHaltedCount() int32 {
	if m != nil {
		return m.HaltedCount
	}
	return 0
}

func (m *ExecSuperstepResult_Stats) GetInactiveCount() int32 {
	if m != nil {
		return m.InactiveCount
	}
	return 0
}

type Aggregator struct {
	Name  string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Id    string `protobuf:"bytes,2,opt,name=id,proto3" json:"id,omitempty"`
	Value []byte `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`
}

func (m *Aggregator) Reset()                    { *m = Aggregator{} }
func (m *Aggregator) String() string            { return proto.CompactTextString(m) }
func (*Aggregator) ProtoMessage()               {}
func (*Aggregator) Descriptor() ([]byte, []int) { return fileDescriptorPrivate, []int{4} }

func (m *Aggregator) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Aggregator) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

func (m *Aggregator) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

type JobCheckpoint struct {
	Superstep   int32                 `protobuf:"varint,1,opt,name=superstep,proto3" json:"superstep,omitempty"`
	Tasks       []*JobCheckpoint_Task `protobuf:"bytes,2,rep,name=tasks" json:"tasks,omitempty"`
	Aggregators []*Aggregator         `protobuf:"bytes,3,rep,name=aggregators" json:"aggregators,omitempty"`
}

func (m *JobCheckpoint) Reset()                    { *m = JobCheckpoint{} }
func (m *JobCheckpoint) String() string            { return proto.CompactTextString(m) }
func (*JobCheckpoint) ProtoMessage()               {}
func (*JobCheckpoint) Descriptor() ([]byte, []int) { return fileDescriptorPrivate, []int{5} }

func (m *JobCheckpoint) GetSuperstep() int32 {
	if m != nil {
		return m.Superstep
	}
	return 0
}

func (m *JobCheckpoint) GetTasks() []*JobCheckpoint_Task {
	if m != nil {
		return m.Tasks
	}
	return nil
}

func (m *JobCheckpoint) GetAggregators() []*Aggregator {
	if m != nil {
		return m.Aggregators
	}
	return nil
}

type JobCheckpoint_Task struct {
	TaskId         int32    `protobuf:"varint,1,opt,name=taskId,proto3" json:"taskId,omitempty"`
	VertexRange    []byte   `protobuf:"bytes,2,opt,name=vertexRange,proto3" json:"vertexRange,omitempty"`
	PreferredHosts []string `protobuf:"bytes,3,rep,name=preferredHosts" json:"preferredHosts,omitempty"`
}

func (m *JobCheckpoint_Task) Reset()                    { *m = JobCheckpoint_Task{} }
func (m *JobCheckpoint_Task) String() string            { return proto.CompactTextString(m) }
func (*JobCheckpoint_Task) ProtoMessage()               {}
func (*JobCheckpoint_Task) Descriptor() ([]byte, []int) { return fileDescriptorPrivate, []int{5, 0} }

func (m *JobCheckpoint_Task) GetTaskId() int32 {
	if m != nil {
		return m.TaskId
	}
	return 0
}

func (m *JobCheckpoint_Task) GetVertexRange() []byte {
	if m != nil {
		return m.VertexRange
	}
	return nil
}

func (m *JobCheckpoint_Task) GetPreferredHosts() []string {
	if m != nil {
		return m.PreferredHosts
	}
	return nil
}

func init() {
	proto.RegisterType((*ExecTaskParams)(nil), "protos.ExecTaskParams")
	proto.RegisterType((*ExecTaskResult)(nil), "protos.ExecTaskResult")
	proto.RegisterType((*ExecSuperstepParams)(nil), "protos.ExecSuperstepParams")
	proto.RegisterType((*ExecSuperstepResult)(nil), "protos.ExecSuperstepResult")
	proto.RegisterType((*ExecSuperstepResult_Stats)(nil), "protos.ExecSuperstepResult.Stats")
	proto.RegisterType((*Aggregator)(nil), "protos.Aggregator")
	proto.RegisterType((*JobCheckpoint)(nil), "protos.JobCheckpoint")
	proto.RegisterType((*JobCheckpoint_Task)(nil), "protos.JobCheckpoint.Task")
}

func init() { proto.RegisterFile("private.proto", fileDescriptorPrivate) }

var fileDescriptorPrivate = []byte{
	// 528 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x9c, 0x54, 0xcf, 0x8e, 0xd3, 0x3e,
	0x18, 0x94, 0xd3, 0xa6, 0x3f, 0xf5, 0x4b, 0xdb, 0xd5, 0xcf, 0x20, 0x14, 0x15, 0x0e, 0x21, 0x42,
	0x28, 0x07, 0x54, 0xa1, 0x82, 0xc4, 0x19, 0x2d, 0x8b, 0x58, 0x24, 0x24, 0xe4, 0xe5, 0x05, 0xdc,
	0xf6, 0x23, 0x0d, 0x4d, 0xe3, 0xc8, 0x76, 0xaa, 0x7d, 0x81, 0x3d, 0xf2, 0xb4, 0xdc, 0xb8, 0x80,
	0x6c, 0xa7, 0x69, 0x12, 0x58, 0xfe, 0x9d, 0x9a, 0x6f, 0x32, 0x1e, 0x8f, 0xa7, 0xe3, 0xc0, 0xb4,
	0x94, 0xd9, 0x81, 0x6b, 0x5c, 0x94, 0x52, 0x68, 0x41, 0x47, 0xf6, 0x47, 0xc5, 0x37, 0x1e, 0xcc,
	0x2e, 0xae, 0x71, 0xfd, 0x81, 0xab, 0xdd, 0x7b, 0x2e, 0xf9, 0x5e, 0xd1, 0x7b, 0x30, 0xd2, 0x5c,
	0xed, 0x2e, 0x37, 0x21, 0x89, 0x48, 0xe2, 0xb3, 0x7a, 0xa2, 0x77, 0xc1, 0xff, 0x24, 0x56, 0x97,
	0x9b, 0xd0, 0x8b, 0x48, 0x32, 0x66, 0x6e, 0x30, 0xa8, 0xd2, 0x42, 0x62, 0x38, 0x70, 0xa8, 0x1d,
	0x68, 0x04, 0x81, 0x7d, 0x70, 0x92, 0xe1, 0x30, 0x22, 0xc9, 0x84, 0xb5, 0x21, 0xfa, 0x00, 0xc6,
	0x3c, 0x4f, 0x85, 0xcc, 0xf4, 0x76, 0x1f, 0xfa, 0x76, 0xed, 0x09, 0xa0, 0x09, 0x9c, 0x35, 0x43,
	0xad, 0x31, 0xb2, 0x1a, 0x7d, 0x98, 0x5e, 0xc0, 0x99, 0xaa, 0x4a, 0x94, 0x4a, 0x63, 0x59, 0x33,
	0xff, 0x8b, 0x48, 0x12, 0x2c, 0xef, 0xbb, 0x93, 0xaa, 0x85, 0x39, 0xde, 0x55, 0x97, 0xc2, 0xfa,
	0x6b, 0xe2, 0x1b, 0x72, 0xca, 0x81, 0xa1, 0xaa, 0x72, 0xfd, 0x97, 0x39, 0xb4, 0x7d, 0x38, 0x01,
	0x9b, 0xc8, 0x6d, 0x3e, 0x1c, 0x85, 0xf5, 0xd7, 0xc4, 0x9f, 0x09, 0xdc, 0xf9, 0x89, 0x61, 0x13,
	0x57, 0x43, 0xad, 0xfd, 0x9c, 0x00, 0x1a, 0xc3, 0xe4, 0x80, 0x52, 0xe3, 0x35, 0xe3, 0x45, 0x8a,
	0x2a, 0xf4, 0xa2, 0x41, 0x32, 0x61, 0x1d, 0x8c, 0x3e, 0x87, 0x80, 0xa7, 0xa9, 0xc4, 0x94, 0x6b,
	0x21, 0x55, 0x38, 0x88, 0x06, 0x49, 0xb0, 0xa4, 0x47, 0x73, 0x2f, 0x9b, 0x57, 0xac, 0x4d, 0x8b,
	0xbf, 0x7a, 0x3d, 0x3f, 0x75, 0x38, 0x3d, 0x35, 0xf2, 0x47, 0x6a, 0xf4, 0x85, 0x29, 0x0b, 0xd7,
	0xca, 0x46, 0x17, 0x2c, 0x1f, 0xfe, 0x22, 0x9a, 0xc5, 0x95, 0x21, 0x32, 0xc7, 0x9f, 0x7f, 0x21,
	0xe0, 0x5b, 0x80, 0x3e, 0x82, 0xa9, 0x16, 0x9a, 0xe7, 0xaf, 0x2a, 0xc9, 0x75, 0x26, 0x8a, 0x3a,
	0x8c, 0x2e, 0x68, 0x58, 0x6b, 0xb1, 0x2f, 0x2b, 0x8d, 0x9b, 0x73, 0x51, 0x15, 0xda, 0x6e, 0xe8,
	0xb3, 0x2e, 0x68, 0x5a, 0x56, 0x03, 0x8d, 0xda, 0xc0, 0xf2, 0xfa, 0x30, 0x7d, 0x02, 0xff, 0x2b,
	0x2c, 0xf4, 0x3b, 0x54, 0x8a, 0xa7, 0xa8, 0x9c, 0xe6, 0xd0, 0x72, 0x7f, 0x7c, 0x61, 0xda, 0xbf,
	0xe5, 0x79, 0xb3, 0xb7, 0x6f, 0x79, 0x6d, 0xc8, 0xf8, 0xcb, 0x0a, 0xbe, 0xd6, 0xd9, 0x01, 0x1d,
	0x67, 0xe4, 0xfc, 0x75, 0xc0, 0xf8, 0x35, 0xc0, 0x29, 0x49, 0x4a, 0x61, 0x58, 0xf0, 0x3d, 0xda,
	0x03, 0x8f, 0x99, 0x7d, 0xa6, 0x33, 0xf0, 0xb2, 0x63, 0x11, 0xbd, 0xcc, 0x76, 0xf3, 0xc0, 0xf3,
	0xca, 0xdd, 0xc6, 0x09, 0x73, 0x43, 0xfc, 0x8d, 0xc0, 0xf4, 0xad, 0x58, 0x9d, 0x6f, 0x71, 0xbd,
	0x2b, 0x45, 0x56, 0xe8, 0xdf, 0xd4, 0xe9, 0x29, 0xf8, 0xa6, 0xeb, 0xae, 0x47, 0xc1, 0x72, 0x7e,
	0xfc, 0x9b, 0x3a, 0x1a, 0x0b, 0x7b, 0x55, 0x1c, 0xf1, 0xdf, 0xca, 0x35, 0xdf, 0xc2, 0xd0, 0x88,
	0xdc, 0x7a, 0xd3, 0x22, 0x08, 0x5a, 0x15, 0xb6, 0xc7, 0x9c, 0xb0, 0x36, 0x44, 0x1f, 0xc3, 0xac,
	0x94, 0xf8, 0x11, 0xa5, 0xc4, 0xcd, 0x1b, 0xa1, 0xb4, 0xdb, 0x7a, 0xcc, 0x7a, 0xe8, 0xca, 0x7d,
	0xee, 0x9e, 0x7d, 0x0f, 0x00, 0x00, 0xff, 0xff, 0xb4, 0x10, 0x0d, 0xb8, 0x06, 0x05, 0x00, 0x00,
}
