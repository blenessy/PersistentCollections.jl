JULIA = julia

.PHONY: test
test: clean
	JULIA_NUM_THREADS=999999 JULIA_DEBUG=Nonsensus $(JULIA) --compiled-modules=no --track-allocation=user -e 'import Pkg; Pkg.activate("."); Pkg.test(coverage=true)'

.PHONY: coverage
coverage:
	# julia -e 'using Pkg; Pkg.add("Coverage")' && brew install lcov
	@mkdir -p ./test/coverage
	$(JULIA) -e 'import Pkg; "Coverage" in keys(Pkg.installed()) || Pkg.add("Coverage"); using Coverage; LCOV.writefile("./test/coverage/lcov.info", process_folder())'
	genhtml -o ./test/coverage ./test/coverage/lcov.info
	open ./test/coverage/index.html

.PHONY: bench
bench:
	BENCH=y $(JULIA) --compiled-modules=no -e 'import Pkg; Pkg.activate("."); Pkg.test()'

.PHONY: clean
clean:
	git clean -fdX
