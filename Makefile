CMD_DIR = cmd
BIN_DIR = $(CMD_DIR)/bin
GO_SRCS = main.go ycsnowflake.go log.go
SRCS = $(addprefix $(CMD_DIR)/, $(GO_SRCS))
PROG = yc_snowflake
CC = go build
all:	
	@if [ ! -d '$(BIN_DIR)' ];then \
	mkdir $(BIN_DIR); \
	fi
	$(CC) -o $(BIN_DIR)/$(PROG) $(SRCS)

run:
	go run $(SRCS)
clean:

