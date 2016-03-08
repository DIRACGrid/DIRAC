.PHONY: test

#clean:
#	rm -rf *.out *.xml htmlcov

S='*System Core Workflow Interfaces'

test: 
	py.test $S --cov

docs: 
	cd docs && make html && cd ..
