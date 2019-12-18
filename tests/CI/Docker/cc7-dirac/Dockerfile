#
#   Simple extension of CC7 with git, openssl and runit
#
#..................................................................
FROM cern/cc7-base:latest
ARG RUNIT_RPM=runit-2.1.2-1.el7.cern.x86_64.rpm

RUN yum install -y git openssl freetype fontconfig pixman libXrender htop psmisc && \
    yum localinstall -y "http://diracproject.web.cern.ch/diracproject/rpm/${RUNIT_RPM}" && \
    yum clean all && \
    rm -rf /var/lib/apt/lists/* /lib/modules/* /lib/firmware/* /lib/kbd /var/cache/yum

CMD ["/usr/sbin/init"]
