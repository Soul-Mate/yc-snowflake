package main

func main() {
	cfg := NewYCSnowflakeConfig()
	ycSf := NewYCSnowflake(cfg)
	ycSf.Start()
}

