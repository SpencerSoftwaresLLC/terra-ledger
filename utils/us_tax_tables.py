US_STATE_TAX_TABLES = {
    "IN": {
        "type": "flat",
        "rate": 0.0295,
    },
    "CA": {
        "type": "progressive",
        "brackets": [
            (0, 0.01),
            (10412, 0.02),
            (24684, 0.04),
            (38959, 0.06),
            (54081, 0.08),
            (68350, 0.093),
        ],
    },
    "TX": {
        "type": "none",
        "rate": 0.0,
    },
    "FL": {
        "type": "none",
        "rate": 0.0,
    },
}

US_LOCAL_TAX_TABLES = {
    "IN": {
        "tippecanoe": 0.0170,
        "marion": 0.0202,
        "hamilton": 0.0110,
        "allen": 0.0148,
        "lake": 0.0150,
    },
    "OH": {
        "columbus": 0.0250,
        "cleveland": 0.0250,
    },
}