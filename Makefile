.PHONY: test

#clean:
#	rm -rf *.out *.xml htmlcov

S=*System

test: 
	py.test $S --cov=$S

docs: 
	cd docs && make html && cd ..
