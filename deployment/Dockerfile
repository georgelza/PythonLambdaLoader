FROM amazon/aws-lambda-python

RUN ulimit -n 1024 && yum -y update && yum -y install \
	python3.9 \
	python3.9-pip \
	python3.9-devel \
	zip \
	&& yum clean all
	
RUN python3 -m pip install pip==23.1.2

RUN pip install virtualenv