package main

func main() {
	ysf := NewYCSnowflake(&YCSnowflakeConfig{})
	ysf.Start()
}

