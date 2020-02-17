package src

type Ringbuffer struct{
	start,use int
	buf []byte
}
func IntToByte(n int) []byte{
	buf:=make([]byte,4)
	buf[3] =  (byte)((n>>24) & 0xFF)
	buf[2] =  (byte)((n>>16) & 0xFF)
	buf[1] =  (byte)((n>>8) & 0xFF)
	buf[0] =  (byte)(n & 0xFF);
	return buf
}
func ByteToInt(buf []byte) int{
	var value int
	value = (int)((buf[0] & 0xFF)|((buf[1] & 0xFF)<<8)|((buf[2] & 0xFF)<<16)|((buf[3] & 0xFF)<<24))
	return value;
}
func NewRingbuffer(size int) *Ringbuffer{
	return &Ringbuffer{0,0,make([]byte,size)}
}//覆盖未读数据包策略
func (r *Ringbuffer) WriteCover(b []byte) bool {
	block_size:=len(b)
	if block_size > 0 {
		size:=len(r.buf)
		start:=(r.start+r.use)%size
		size_byte:= IntToByte(block_size)
		/*判断是非会覆盖未读block，
		  是的话，修改r.start */
		flag:=block_size+len(size_byte)
		for flag > (r.start-start+size)%size &&r.use!=0 {
			rblock_size:= ByteToInt(r.buf[r.start:r.start+4])
			r.start = (r.start + rblock_size + 4)%size
		}
		//保存block的长度
		n:=copy(r.buf[start:],size_byte)
		if start+len(size_byte) > len(r.buf){
			copy(r.buf,size_byte[n:])//判断是否需要绕回
		}
		start = (start+len(size_byte))%size
		//保存block的内容
		n =copy(r.buf[start:],b)
		if start+len(b) > len(r.buf){
			copy(r.buf,b[n:])//判断是非需要绕回
		}
		start = (start+block_size)%size
		//更新ringbuffer的使用量
		r.use = (start+size-r.start)%size
		return true
	}
	return false
}
//丢弃新写入策略
func (r *Ringbuffer) Write(b []byte) bool {
	block_size:=len(b)
	if block_size > 0 {
		size:=len(r.buf)
		start:=(r.start+r.use)%size
		size_byte:= IntToByte(block_size)
		//判断ringbuffer是否还有空间存放block
		end:=(start+len(b)+len(size_byte))
		flag:=end-len(r.buf)
		if flag>0 && flag > r.start {
			return false
		}
		//保存block的长度
		n:=copy(r.buf[start:],size_byte)
		if start+len(size_byte) > len(r.buf){
			copy(r.buf,size_byte[n:])
		}
		start = (start+len(size_byte))%size
		//保存block的内容
		n =copy(r.buf[start:],b)
		if start+len(b) > len(r.buf){
			copy(r.buf,b[n:])
		}
		start = (start+block_size)%size
		//更新ringbuffer的使用量
		r.use = (start+size-r.start)%size
		return true
	}
	return false
}
func (r *Ringbuffer) Read(b []byte) int{
	if r.use > 0 {//判断是非还有未读数据
		//获取block的长度
		size_byte:=make([]byte,4)
		t:=copy(size_byte,r.buf[r.start:])
		if t!=4 {//判断有没有被分割开
			copy(size_byte[t:4],r.buf[:])
		}
		rblock_size:= ByteToInt(size_byte)
		//获取block的内容
		start:=(r.start+4)%len(r.buf)
		nread:=0
		if start+rblock_size >= len(r.buf) {
			n:= copy(b,r.buf[start:])//判断数据包内容有没有被分割
			nread = copy(b[n:],r.buf[:])
			nread = nread + n
		}else{
			nread = copy(b,r.buf[start:start+rblock_size])
		}
		if nread == rblock_size {
			r.start = (r.start + rblock_size + 4)%len(r.buf)
			r.use = r.use - rblock_size - 4
			return nread
		}else{
			return -1
		}
	}
	return 0
}
func (r *Ringbuffer) GetUse() int{
	return r.use
}

