all:im_bin ims_bin imr_bin 


im_bin:
	cd im && make

ims_bin:
	cd ims && make

imr_bin:
	cd imr && make


install:all
	cp ./im/im ./bin
	cp ./ims/ims ./bin
	cp ./imr/imr ./bin

clean:
	rm -f ./im/im  ./im/benchmark ./im/benchmark_connection ./im/benchmark_sender ./im/benchmark_storage ./im/benchmark_route ./ims/main.test ./ims/ims ./imr/imr
