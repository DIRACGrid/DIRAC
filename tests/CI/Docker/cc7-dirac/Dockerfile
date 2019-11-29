#
#   Simple extension of CC7 with git, openssl and runit
#
#..................................................................

FROM cern/cc7-base:latest
ARG RUNIT_RPM=runit-2.1.2-1.el7.cern.x86_64.rpm

RUN yum install -y git openssl
RUN yum localinstall -y http://diracproject.web.cern.ch/diracproject/rpm/runit-2.1.2-1.el7.cern.x86_64.rpm

CMD ["/usr/sbin/init"]
