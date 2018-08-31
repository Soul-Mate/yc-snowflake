package main

var (
	enableTLS      *bool
	caCertFile     *string
	clientCert     *string
	clientKey      *string
	clientCertAuth *bool
	workerID       *int
)


func main() {
	cfg := NewYCSnowflakeConfig()
	ycSf := NewYCSnowflake(cfg)
	ycSf.Start()
}

