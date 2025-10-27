# ==========================================================
# Makefile for CS525 Assignment 4 - B+ Tree
# ==========================================================

CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -g

# 可执行目标
TARGET = test_assign4
EXPRTEST = test_expr

# 公共模块（从上次作业继承）
SRCS_COMMON = \
    storage_mgr.c \
    dberror.c \
    buffer_mgr.c \
    buffer_mgr_stat.c \
    record_mgr.c \
    rm_serializer.c \
    expr.c

# B+ 树实现 & 测试文件
BTREE_SRCS = \
    btree_mgr.c \
    test_assign4_1.c

EXPR_SRCS = \
    test_expr.c

# 自动生成对象文件
OBJS_COMMON = $(SRCS_COMMON:.c=.o)
BTREE_OBJS = $(BTREE_SRCS:.c=.o)
EXPR_OBJS = $(EXPR_SRCS:.c=.o)

# ==========================================================
# 构建规则
# ==========================================================
all: $(TARGET) $(EXPRTEST)

$(TARGET): $(OBJS_COMMON) $(BTREE_OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS_COMMON) $(BTREE_OBJS)

$(EXPRTEST): $(OBJS_COMMON) $(EXPR_OBJS)
	$(CC) $(CFLAGS) -o $(EXPRTEST) $(OBJS_COMMON) $(EXPR_OBJS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

run: $(TARGET)
	./$(TARGET)

run-expr: $(EXPRTEST)
	./$(EXPRTEST)

clean:
	rm -f $(TARGET) $(EXPRTEST) *.o *.out

valgrind:
	valgrind --leak-check=full ./$(TARGET)
