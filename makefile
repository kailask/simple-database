include ./makefile.inc

all: ./test/test

./test/test.o: ./qe/qe.h

./test/test: ./test/test.o ./qe/libqe.a ./ix/libix.a ./rm/librm.a ./rbf/librbf.a

# dependencies to compile used libraries
.PHONY: ./rbf/librbf.a
./rbf/librbf.a:
	$(MAKE) -C ./rbf librbf.a

.PHONY: ./rm/librm.a
./rm/librm.a:
	$(MAKE) -C ./rm librm.a

.PHONY: ./ix/libix.a
./ix/libix.a:
	$(MAKE) -C ./ix libix.a

.PHONY: ./qe/libqe.a
./qe/libqe.a:
	$(MAKE) -C ./qe libqe.a

.PHONY: clean
clean:
	-rm Tables* Columns* left* right* Indexes*
	-rm test/*.o test/test
	-$(MAKE) -C ./qe clean

.PHONY: test
test:
	$(MAKE) -s
	-test/test
	$(MAKE) -s clean