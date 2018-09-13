package main

import "github.com/Soul-Mate/yc-snowflake/snowflake"

func main() {
	sf := snowflake.New(2)
	uuid := sf.Gen()
	println(uuid)
}
