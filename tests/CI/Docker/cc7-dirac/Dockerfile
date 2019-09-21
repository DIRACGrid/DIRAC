#
#   Simple extension of CC7 with git, openssl and runit
#
#..................................................................
FROM cern/cc7-base

RUN yum install -y git openssl && \
    yum localinstall -y http://diracproject.web.cern.ch/diracproject/rpm/runit-2.1.2-1.el7.cern.x86_64.rpm && \
    yum clean all && \
    rm -rf /var/lib/apt/lists/* /lib/modules/* /lib/firmware/* /lib/kbd /var/cache/yum

CMD ["/usr/sbin/init"]
