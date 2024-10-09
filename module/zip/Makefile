# Define the compiler and flags
CC = gcc
CFLAGS = -Wall -Wextra -O2

# Define the target executable
TARGET = zip_module

# Define the source files
SRCS = $(wildcard *.c)

# Define the object files
OBJS = $(SRCS:.c=.o)

# Default target
all: $(TARGET)

# Link the object files to create the executable
$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

# Compile the source files into object files
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Clean up the build files
clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean