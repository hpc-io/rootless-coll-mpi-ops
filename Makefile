C_FLAGS = -g
LIB_FLAGS = 
CC  = mpicc
C_SRC = rootless_ops.c
C_EXE   = demo

.SUFFIXES: .cpp .c
C_OBJS = $(C_SRC:.c=.o)

all:    ${C_EXE}

$(C_EXE): $(C_OBJS)
	$(CC) $(C_FLAGS) $(LIB_FLAGS) $(C_OBJS) -o ${C_EXE}

#C_EXE: rootless_ops.c
#	mpicc -g rootless_ops.c -o demo
clean:
	\rm -f *.o ${C_EXE}	
