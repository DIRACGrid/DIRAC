CREATE table activities (
    id INTEGER  PRIMARY KEY,
    sourceId INTEGER,
    name TEXT,
    category TEXT,
    unit TEXT,
    type TEXT,
    description TEXT,
    bucketLength INTEGER,
    filename TEXT,
    lastUpdate TEXT
);
CREATE table sources (
    id INTEGER  PRIMARY KEY,
    setup TEXT,
    site TEXT,
    componentType TEXT,
    componentLocation TEXT,
    componentName TEXT
);
CREATE table views (
    id INTEGER PRIMARY KEY,
    name TEXT,
    definition TEXT,
    variableFields TEXT
);
