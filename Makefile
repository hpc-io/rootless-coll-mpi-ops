C_FLAGS = -g
LIB_FLAGS = 
CC  = mpicc
C_SRC = rootless_ops.c testcases.c
C_EXE   = demo
SO_TARGET=librlo.so
.SUFFIXES: .cpp .c
C_OBJS = $(C_SRC:.c=.o)

all:    ${C_EXE} static

so:
	$(CC) $(C_FLAGS) -shared -fPIC -o $(SO_TARGET) rootless_ops.c
static:
	$(CC) $(C_FLAGS) -c -o librlo.a rootless_ops.c
$(C_EXE): $(C_OBJS)
	$(CC) $(C_FLAGS) $(LIB_FLAGS) $(C_OBJS) -o ${C_EXE}

$(C_OBJS): $(C_SRC)
	$(CC) -c $(C_FLAGS) $(C_SRC) #-o $(C_OBJS)




#C_EXE: rootless_ops.c
#	mpicc -g rootless_ops.c -o demo
clean:
	rm -f *.o *.so *.a ${C_EXE}	
