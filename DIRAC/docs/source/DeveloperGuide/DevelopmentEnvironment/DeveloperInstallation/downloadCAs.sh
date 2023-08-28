#!/usr/bin/env bash

for ca in $(curl http://repository.egi.eu/sw/production/cas/1/current/ca-policy-egi-core.list | grep '^ca_' | sed 's:-[1-9]*$::g')
do
  curl http://repository.egi.eu/sw/production/cas/1/current/tgz/$ca.tar.gz | tar xz
  mv $ca/* .
  rmdir $ca
done
