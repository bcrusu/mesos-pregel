// Code generated by protoc-gen-gogo.
// source: service.proto
// DO NOT EDIT!

/*
Package protos is a generated protocol buffer package.

It is generated from these files:
	service.proto

It has these top-level messages:
	JobIdRequest
	SimpleCallReply
	CreateJobRequest
	CreateJobReply
	GetJobStatusReply
	GetJobResultReply
*/
package protos

import proto "github.com/gogo/protobuf/proto"
import fmt "fmt"
import math "math"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion2 // please upgrade the proto package

type JobStatus int32

const (
	JobStatus_Created   JobStatus = 0
	JobStatus_Running   JobStatus = 1
	JobStatus_Completed JobStatus = 2
	JobStatus_Cancelled JobStatus = 3
	JobStatus_Failed    JobStatus = 4
)

var JobStatus_name = map[int32]string{
	0: "Created",
	1: "Running",
	2: "Completed",
	3: "Cancelled",
	4: "Failed",
}
var JobStatus_value = map[string]int32{
	"Created":   0,
	"Running":   1,
	"Completed": 2,
	"Cancelled": 3,
	"Failed":    4,
}

func (x JobStatus) String() string {
	return proto.EnumName(JobStatus_name, int32(x))
}
func (JobStatus) EnumDescriptor() ([]byte, []int) { return fileDescriptorService, []int{0} }

type CallStatus int32

const (
	CallStatus_OK                    CallStatus = 0
	CallStatus_INTERNAL_ERROR        CallStatus = 1
	CallStatus_ERROR_INVALID_JOB     CallStatus = 2
	CallStatus_ERROR_INVALID_REQUEST CallStatus = 3
)

var CallStatus_name = map[int32]string{
	0: "OK",
	1: "INTERNAL_ERROR",
	2: "ERROR_INVALID_JOB",
	3: "ERROR_INVALID_REQUEST",
}
var CallStatus_value = map[string]int32{
	"OK":                    0,
	"INTERNAL_ERROR":        1,
	"ERROR_INVALID_JOB":     2,
	"ERROR_INVALID_REQUEST": 3,
}

func (x CallStatus) String() string {
	return proto.EnumName(CallStatus_name, int32(x))
}
func (CallStatus) EnumDescriptor() ([]byte, []int) { return fileDescriptorService, []int{1} }

type JobIdRequest struct {
	JobId string `protobuf:"bytes,1,opt,name=jobId,proto3" json:"jobId,omitempty"`
}

func (m *JobIdRequest) Reset()                    { *m = JobIdRequest{} }
func (m *JobIdRequest) String() string            { return proto.CompactTextString(m) }
func (*JobIdRequest) ProtoMessage()               {}
func (*JobIdRequest) Descriptor() ([]byte, []int) { return fileDescriptorService, []int{0} }

func (m *JobIdRequest) GetJobId() string {
	if m != nil {
		return m.JobId
	}
	return ""
}

type SimpleCallReply struct {
	Status CallStatus `protobuf:"varint,1,opt,name=status,proto3,enum=protos.CallStatus" json:"status,omitempty"`
}

func (m *SimpleCallReply) Reset()                    { *m = SimpleCallReply{} }
func (m *SimpleCallReply) String() string            { return proto.CompactTextString(m) }
func (*SimpleCallReply) ProtoMessage()               {}
func (*SimpleCallReply) Descriptor() ([]byte, []int) { return fileDescriptorService, []int{1} }

func (m *SimpleCallReply) GetStatus() CallStatus {
	if m != nil {
		return m.Status
	}
	return CallStatus_OK
}

type CreateJobRequest struct {
	Label             string  `protobuf:"bytes,1,opt,name=label,proto3" json:"label,omitempty"`
	Store             string  `protobuf:"bytes,2,opt,name=store,proto3" json:"store,omitempty"`
	StoreParams       []byte  `protobuf:"bytes,3,opt,name=storeParams,proto3" json:"storeParams,omitempty"`
	Algorithm         string  `protobuf:"bytes,4,opt,name=algorithm,proto3" json:"algorithm,omitempty"`
	AlgorithmParams   []byte  `protobuf:"bytes,5,opt,name=algorithmParams,proto3" json:"algorithmParams,omitempty"`
	TaskCPU           float64 `protobuf:"fixed64,6,opt,name=taskCPU,proto3" json:"taskCPU,omitempty"`
	TaskMEM           float64 `protobuf:"fixed64,7,opt,name=taskMEM,proto3" json:"taskMEM,omitempty"`
	TaskVertices      int32   `protobuf:"varint,8,opt,name=taskVertices,proto3" json:"taskVertices,omitempty"`
	TaskTimeout       int64   `protobuf:"varint,9,opt,name=taskTimeout,proto3" json:"taskTimeout,omitempty"`
	TaskMaxRetryCount int32   `protobuf:"varint,10,opt,name=taskMaxRetryCount,proto3" json:"taskMaxRetryCount,omitempty"`
}

func (m *CreateJobRequest) Reset()                    { *m = CreateJobRequest{} }
func (m *CreateJobRequest) String() string            { return proto.CompactTextString(m) }
func (*CreateJobRequest) ProtoMessage()               {}
func (*CreateJobRequest) Descriptor() ([]byte, []int) { return fileDescriptorService, []int{2} }

func (m *CreateJobRequest) GetLabel() string {
	if m != nil {
		return m.Label
	}
	return ""
}

func (m *CreateJobRequest) GetStore() string {
	if m != nil {
		return m.Store
	}
	return ""
}

func (m *CreateJobRequest) GetStoreParams() []byte {
	if m != nil {
		return m.StoreParams
	}
	return nil
}

func (m *CreateJobRequest) GetAlgorithm() string {
	if m != nil {
		return m.Algorithm
	}
	return ""
}

func (m *CreateJobRequest) GetAlgorithmParams() []byte {
	if m != nil {
		return m.AlgorithmParams
	}
	return nil
}

func (m *CreateJobRequest) GetTaskCPU() float64 {
	if m != nil {
		return m.TaskCPU
	}
	return 0
}

func (m *CreateJobRequest) GetTaskMEM() float64 {
	if m != nil {
		return m.TaskMEM
	}
	return 0
}

func (m *CreateJobRequest) GetTaskVertices() int32 {
	if m != nil {
		return m.TaskVertices
	}
	return 0
}

func (m *CreateJobRequest) GetTaskTimeout() int64 {
	if m != nil {
		return m.TaskTimeout
	}
	return 0
}

func (m *CreateJobRequest) GetTaskMaxRetryCount() int32 {
	if m != nil {
		return m.TaskMaxRetryCount
	}
	return 0
}

type CreateJobReply struct {
	Status CallStatus `protobuf:"varint,1,opt,name=status,proto3,enum=protos.CallStatus" json:"status,omitempty"`
	JobId  string     `protobuf:"bytes,2,opt,name=jobId,proto3" json:"jobId,omitempty"`
}

func (m *CreateJobReply) Reset()                    { *m = CreateJobReply{} }
func (m *CreateJobReply) String() string            { return proto.CompactTextString(m) }
func (*CreateJobReply) ProtoMessage()               {}
func (*CreateJobReply) Descriptor() ([]byte, []int) { return fileDescriptorService, []int{3} }

func (m *CreateJobReply) GetStatus() CallStatus {
	if m != nil {
		return m.Status
	}
	return CallStatus_OK
}

func (m *CreateJobReply) GetJobId() string {
	if m != nil {
		return m.JobId
	}
	return ""
}

type GetJobStatusReply struct {
	Status      CallStatus `protobuf:"varint,1,opt,name=status,proto3,enum=protos.CallStatus" json:"status,omitempty"`
	JobStatus   JobStatus  `protobuf:"varint,2,opt,name=jobStatus,proto3,enum=protos.JobStatus" json:"jobStatus,omitempty"`
	Superstep   int32      `protobuf:"varint,3,opt,name=superstep,proto3" json:"superstep,omitempty"`
	PercentDone int32      `protobuf:"varint,4,opt,name=percentDone,proto3" json:"percentDone,omitempty"`
}

func (m *GetJobStatusReply) Reset()                    { *m = GetJobStatusReply{} }
func (m *GetJobStatusReply) String() string            { return proto.CompactTextString(m) }
func (*GetJobStatusReply) ProtoMessage()               {}
func (*GetJobStatusReply) Descriptor() ([]byte, []int) { return fileDescriptorService, []int{4} }

func (m *GetJobStatusReply) GetStatus() CallStatus {
	if m != nil {
		return m.Status
	}
	return CallStatus_OK
}

func (m *GetJobStatusReply) GetJobStatus() JobStatus {
	if m != nil {
		return m.JobStatus
	}
	return JobStatus_Created
}

func (m *GetJobStatusReply) GetSuperstep() int32 {
	if m != nil {
		return m.Superstep
	}
	return 0
}

func (m *GetJobStatusReply) GetPercentDone() int32 {
	if m != nil {
		return m.PercentDone
	}
	return 0
}

type GetJobResultReply struct {
	Status CallStatus `protobuf:"varint,1,opt,name=status,proto3,enum=protos.CallStatus" json:"status,omitempty"`
	JobId  string     `protobuf:"bytes,2,opt,name=jobId,proto3" json:"jobId,omitempty"`
	Result []byte     `protobuf:"bytes,3,opt,name=result,proto3" json:"result,omitempty"`
}

func (m *GetJobResultReply) Reset()                    { *m = GetJobResultReply{} }
func (m *GetJobResultReply) String() string            { return proto.CompactTextString(m) }
func (*GetJobResultReply) ProtoMessage()               {}
func (*GetJobResultReply) Descriptor() ([]byte, []int) { return fileDescriptorService, []int{5} }

func (m *GetJobResultReply) GetStatus() CallStatus {
	if m != nil {
		return m.Status
	}
	return CallStatus_OK
}

func (m *GetJobResultReply) GetJobId() string {
	if m != nil {
		return m.JobId
	}
	return ""
}

func (m *GetJobResultReply) GetResult() []byte {
	if m != nil {
		return m.Result
	}
	return nil
}

func init() {
	proto.RegisterType((*JobIdRequest)(nil), "protos.JobIdRequest")
	proto.RegisterType((*SimpleCallReply)(nil), "protos.SimpleCallReply")
	proto.RegisterType((*CreateJobRequest)(nil), "protos.CreateJobRequest")
	proto.RegisterType((*CreateJobReply)(nil), "protos.CreateJobReply")
	proto.RegisterType((*GetJobStatusReply)(nil), "protos.GetJobStatusReply")
	proto.RegisterType((*GetJobResultReply)(nil), "protos.GetJobResultReply")
	proto.RegisterEnum("protos.JobStatus", JobStatus_name, JobStatus_value)
	proto.RegisterEnum("protos.CallStatus", CallStatus_name, CallStatus_value)
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// Client API for Service service

type ServiceClient interface {
	CreateJob(ctx context.Context, in *CreateJobRequest, opts ...grpc.CallOption) (*CreateJobReply, error)
	CancelJob(ctx context.Context, in *JobIdRequest, opts ...grpc.CallOption) (*SimpleCallReply, error)
	GetJobStatus(ctx context.Context, in *JobIdRequest, opts ...grpc.CallOption) (*GetJobStatusReply, error)
	GetJobResult(ctx context.Context, in *JobIdRequest, opts ...grpc.CallOption) (*GetJobResultReply, error)
}

type serviceClient struct {
	cc *grpc.ClientConn
}

func NewServiceClient(cc *grpc.ClientConn) ServiceClient {
	return &serviceClient{cc}
}

func (c *serviceClient) CreateJob(ctx context.Context, in *CreateJobRequest, opts ...grpc.CallOption) (*CreateJobReply, error) {
	out := new(CreateJobReply)
	err := grpc.Invoke(ctx, "/protos.Service/CreateJob", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serviceClient) CancelJob(ctx context.Context, in *JobIdRequest, opts ...grpc.CallOption) (*SimpleCallReply, error) {
	out := new(SimpleCallReply)
	err := grpc.Invoke(ctx, "/protos.Service/CancelJob", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serviceClient) GetJobStatus(ctx context.Context, in *JobIdRequest, opts ...grpc.CallOption) (*GetJobStatusReply, error) {
	out := new(GetJobStatusReply)
	err := grpc.Invoke(ctx, "/protos.Service/GetJobStatus", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serviceClient) GetJobResult(ctx context.Context, in *JobIdRequest, opts ...grpc.CallOption) (*GetJobResultReply, error) {
	out := new(GetJobResultReply)
	err := grpc.Invoke(ctx, "/protos.Service/GetJobResult", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for Service service

type ServiceServer interface {
	CreateJob(context.Context, *CreateJobRequest) (*CreateJobReply, error)
	CancelJob(context.Context, *JobIdRequest) (*SimpleCallReply, error)
	GetJobStatus(context.Context, *JobIdRequest) (*GetJobStatusReply, error)
	GetJobResult(context.Context, *JobIdRequest) (*GetJobResultReply, error)
}

func RegisterServiceServer(s *grpc.Server, srv ServiceServer) {
	s.RegisterService(&_Service_serviceDesc, srv)
}

func _Service_CreateJob_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateJobRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServiceServer).CreateJob(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/protos.Service/CreateJob",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServiceServer).CreateJob(ctx, req.(*CreateJobRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Service_CancelJob_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(JobIdRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServiceServer).CancelJob(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/protos.Service/CancelJob",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServiceServer).CancelJob(ctx, req.(*JobIdRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Service_GetJobStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(JobIdRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServiceServer).GetJobStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/protos.Service/GetJobStatus",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServiceServer).GetJobStatus(ctx, req.(*JobIdRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Service_GetJobResult_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(JobIdRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServiceServer).GetJobResult(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/protos.Service/GetJobResult",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServiceServer).GetJobResult(ctx, req.(*JobIdRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Service_serviceDesc = grpc.ServiceDesc{
	ServiceName: "protos.Service",
	HandlerType: (*ServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateJob",
			Handler:    _Service_CreateJob_Handler,
		},
		{
			MethodName: "CancelJob",
			Handler:    _Service_CancelJob_Handler,
		},
		{
			MethodName: "GetJobStatus",
			Handler:    _Service_GetJobStatus_Handler,
		},
		{
			MethodName: "GetJobResult",
			Handler:    _Service_GetJobResult_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "service.proto",
}

func init() { proto.RegisterFile("service.proto", fileDescriptorService) }

var fileDescriptorService = []byte{
	// 581 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0xac, 0x54, 0xc1, 0x6e, 0xd3, 0x4c,
	0x10, 0x8e, 0x9d, 0xc6, 0xf9, 0x3d, 0x4d, 0x53, 0x67, 0xd5, 0xf6, 0x77, 0x2b, 0x0e, 0x96, 0xc5,
	0xc1, 0x8a, 0x50, 0x91, 0xc2, 0x15, 0x84, 0x42, 0x6a, 0x50, 0x42, 0x93, 0x94, 0x4d, 0xda, 0x03,
	0x97, 0xc8, 0x49, 0x46, 0xc5, 0xc5, 0xb1, 0x8d, 0x77, 0x8d, 0xc8, 0x4b, 0xf0, 0x2c, 0x3c, 0x0a,
	0x8f, 0x84, 0x76, 0x1d, 0xc7, 0x6e, 0x0a, 0x12, 0x95, 0x38, 0x79, 0xbe, 0x6f, 0xc6, 0xdf, 0xee,
	0xce, 0x37, 0xbb, 0x70, 0xc0, 0x30, 0xf9, 0xea, 0x2f, 0xf0, 0x3c, 0x4e, 0x22, 0x1e, 0x11, 0x4d,
	0x7e, 0x98, 0xfd, 0x14, 0x1a, 0x83, 0x68, 0xde, 0x5f, 0x52, 0xfc, 0x92, 0x22, 0xe3, 0xe4, 0x08,
	0x6a, 0x77, 0x02, 0x9b, 0x8a, 0xa5, 0x38, 0x3a, 0xcd, 0x80, 0xfd, 0x0a, 0x0e, 0x27, 0xfe, 0x2a,
	0x0e, 0xb0, 0xe7, 0x05, 0x01, 0xc5, 0x38, 0x58, 0x93, 0x36, 0x68, 0x8c, 0x7b, 0x3c, 0x65, 0xb2,
	0xb2, 0xd9, 0x21, 0x99, 0x30, 0x3b, 0x17, 0x25, 0x13, 0x99, 0xa1, 0x9b, 0x0a, 0xfb, 0xa7, 0x0a,
	0x46, 0x2f, 0x41, 0x8f, 0xe3, 0x20, 0x9a, 0x97, 0x56, 0x0a, 0xbc, 0x39, 0x06, 0xf9, 0x4a, 0x12,
	0x08, 0x96, 0xf1, 0x28, 0x41, 0x53, 0xcd, 0x58, 0x09, 0x88, 0x05, 0xfb, 0x32, 0xb8, 0xf2, 0x12,
	0x6f, 0xc5, 0xcc, 0xaa, 0xa5, 0x38, 0x0d, 0x5a, 0xa6, 0xc8, 0x13, 0xd0, 0xbd, 0xe0, 0x36, 0x4a,
	0x7c, 0xfe, 0x69, 0x65, 0xee, 0xc9, 0x7f, 0x0b, 0x82, 0x38, 0x70, 0xb8, 0x05, 0x1b, 0x8d, 0x9a,
	0xd4, 0xd8, 0xa5, 0x89, 0x09, 0x75, 0xee, 0xb1, 0xcf, 0xbd, 0xab, 0x6b, 0x53, 0xb3, 0x14, 0x47,
	0xa1, 0x39, 0xcc, 0x33, 0x43, 0x77, 0x68, 0xd6, 0x8b, 0xcc, 0xd0, 0x1d, 0x12, 0x1b, 0x1a, 0x22,
	0xbc, 0xc1, 0x84, 0xfb, 0x0b, 0x64, 0xe6, 0x7f, 0x96, 0xe2, 0xd4, 0xe8, 0x3d, 0x4e, 0x9c, 0x40,
	0xe0, 0xa9, 0xbf, 0xc2, 0x28, 0xe5, 0xa6, 0x6e, 0x29, 0x4e, 0x95, 0x96, 0x29, 0xf2, 0x0c, 0x5a,
	0x52, 0xd0, 0xfb, 0x46, 0x91, 0x27, 0xeb, 0x5e, 0x94, 0x86, 0xdc, 0x04, 0x29, 0xf5, 0x30, 0x61,
	0x53, 0x68, 0x96, 0x3a, 0xfa, 0x48, 0x43, 0x0a, 0x97, 0xd5, 0xb2, 0xcb, 0x3f, 0x14, 0x68, 0xbd,
	0x43, 0x3e, 0x88, 0xe6, 0x9b, 0xf2, 0x47, 0xeb, 0x3e, 0x07, 0xfd, 0x2e, 0xff, 0x5b, 0x6a, 0x37,
	0x3b, 0xad, 0xbc, 0xbc, 0x90, 0x2d, 0x6a, 0x84, 0x6d, 0x2c, 0x8d, 0x31, 0x61, 0x1c, 0x63, 0x69,
	0x6b, 0x8d, 0x16, 0x84, 0x68, 0x5a, 0x8c, 0xc9, 0x02, 0x43, 0x7e, 0x11, 0x85, 0x28, 0x6d, 0xad,
	0xd1, 0x32, 0x65, 0xaf, 0xf2, 0x1d, 0x53, 0x64, 0x69, 0xc0, 0xff, 0x51, 0x27, 0xc8, 0x09, 0x68,
	0x89, 0x14, 0xdc, 0x8c, 0xda, 0x06, 0xb5, 0xc7, 0xa0, 0x6f, 0x8f, 0x41, 0xf6, 0xa1, 0x9e, 0x59,
	0xb0, 0x34, 0x2a, 0x02, 0xd0, 0x34, 0x0c, 0xfd, 0xf0, 0xd6, 0x50, 0xc8, 0x01, 0xe8, 0xbd, 0x48,
	0x5c, 0x17, 0x91, 0x53, 0x25, 0xf4, 0xc2, 0x05, 0x06, 0x01, 0x2e, 0x8d, 0x2a, 0x01, 0xd0, 0xde,
	0x7a, 0xbe, 0x88, 0xf7, 0xda, 0x1f, 0x01, 0x8a, 0x4d, 0x11, 0x0d, 0xd4, 0xf1, 0x7b, 0xa3, 0x42,
	0x08, 0x34, 0xfb, 0xa3, 0xa9, 0x4b, 0x47, 0xdd, 0xcb, 0x99, 0x4b, 0xe9, 0x98, 0x1a, 0x0a, 0x39,
	0x86, 0x96, 0x0c, 0x67, 0xfd, 0xd1, 0x4d, 0xf7, 0xb2, 0x7f, 0x31, 0x1b, 0x8c, 0xdf, 0x18, 0x2a,
	0x39, 0x85, 0xe3, 0xfb, 0x34, 0x75, 0x3f, 0x5c, 0xbb, 0x93, 0xa9, 0x51, 0xed, 0x7c, 0x57, 0xa1,
	0x3e, 0xc9, 0x2e, 0x3d, 0x79, 0x0d, 0xfa, 0x76, 0x5c, 0x88, 0xb9, 0xed, 0xc7, 0xce, 0x9d, 0x3c,
	0x3b, 0xf9, 0x4d, 0x26, 0x0e, 0xd6, 0x76, 0x85, 0xbc, 0xcc, 0xcf, 0x20, 0x04, 0x8e, 0x4a, 0x9e,
	0x6e, 0x9f, 0x8e, 0xb3, 0xff, 0x73, 0x76, 0xe7, 0xa9, 0xb0, 0x2b, 0xa4, 0x0b, 0x8d, 0xf2, 0x60,
	0xfd, 0x41, 0xe0, 0x34, 0x67, 0x1f, 0x0c, 0x61, 0x59, 0x22, 0x73, 0xfa, 0xef, 0x24, 0x4a, 0x53,
	0x61, 0x57, 0xe6, 0xd9, 0x9b, 0xf7, 0xe2, 0x57, 0x00, 0x00, 0x00, 0xff, 0xff, 0x9c, 0xdf, 0xa7,
	0x99, 0x0b, 0x05, 0x00, 0x00,
}
