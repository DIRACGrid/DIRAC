CREATE table activities (
	id INTEGER  PRIMARY KEY,
    sourceId INTEGER,
    name TEXT,
    category TEXT,
    unit TEXT,
    operation TEXT,
    buckets INTEGER,
    filename TEXT
);
CREATE table sources (
    id INTEGER  PRIMARY KEY,
    setup TEXT,
    site TEXT,
    componentType TEXT,
    componentLocation TEXT,
    componentName TEXT
);

