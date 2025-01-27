package server

type ServerSummary struct {
	nconnections      int64
	nclients          int64
	clientset_count   int64 //重复uid的client对象不计数
	in_message_count  int64
	out_message_count int64
}

func NewServerSummary() *ServerSummary {
	s := new(ServerSummary)
	return s
}
