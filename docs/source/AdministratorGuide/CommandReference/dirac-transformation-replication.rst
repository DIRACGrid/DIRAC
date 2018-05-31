================================
dirac-transformation-replication
================================

Create one replication transformation for each MetaValue given

Is running in dry-run mode, unless enabled with -x

MetaValue and TargetSEs can be comma separated lists::

  dirac-transformation-replication <MetaValue1[,val2,val3]> <TargetSEs> [-G<Files>] [-S<SourceSEs>][-N<ExtraName>] [-T<Type>] [-M<Key>] [-K...] -x

Options::

  -G  --GroupSize <value>      : Number of Files per transformation task
  -S  --SourceSEs <value>      : SourceSE(s) to use, comma separated list
  -N  --Extraname <value>      : String to append to transformation name
  -P  --Plugin <value>         : Plugin to use for transformation
  -T  --Flavour <value>        : Flavour to create: Replication or Moving
  -K  --MetaKey <value>        : Meta Key to use: TransformationID
  -M  --MetaData <value>       : MetaData to use Key/Value Pairs: 'DataType:REC,'
  -x  --Enable                 : Enable the transformation creation, otherwise dry-run

