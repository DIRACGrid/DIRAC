.PHONY: test
#.PHONY: clean, install, docs, test

#clean:
#	rm -rf *.out *.xml htmlcov

#install:
#	virtualenv venv && \
#		source venv/bin/activate && \
#		pip install -r requirements.txt

#docs: install
#	cd docs && make html && cd ..

activate:
	workon DIRACGrid

test: activate
	py.test *System --cov=.
