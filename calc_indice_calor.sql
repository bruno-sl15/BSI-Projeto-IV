CREATE OR REPLACE STREAM "STATE_STREAM_1" (
    CIDADE VARCHAR(16),
    TEMP_AR_C FLOAT,
    TEMP_AR_F FLOAT,
	UMD_AR INTEGER
);

CREATE OR REPLACE PUMP "STATE_PUMP_1" AS
    INSERT INTO "STATE_STREAM_1"
        SELECT STREAM
			DC_NOME,
			TEM_INS,
			(TEM_INS * 1.8 + 32),
			UMD_INS
		FROM "SOURCE_SQL_STREAM_001"
;

CREATE OR REPLACE STREAM "STATE_STREAM_2" (
    CIDADE VARCHAR(16),
    TEMP_AR_C FLOAT,
    TEMP_AR_F FLOAT,
    UMD_AR INTEGER,
    INDICE_CALOR_F NUMERIC(10,2)
);

CREATE OR REPLACE PUMP "STATE_PUMP_2" AS
    INSERT INTO "STATE_STREAM_2"
        SELECT STREAM
            CIDADE,
            TEMP_AR_C,
            TEMP_AR_F,
            UMD_AR,
            CASE
                WHEN (TEMP_AR_F * 1.1 - 10.3 + UMD_AR * 0.047) < 80
                    THEN (TEMP_AR_F * 1.1 - 10.3 + UMD_AR * 0.047)
                WHEN (TEMP_AR_F >= 80 AND TEMP_AR_F <= 112 AND UMD_AR <= 13)
                    THEN (
                        - 42.379 + 2.04901523 * TEMP_AR_F + 10.14333127 * UMD_AR
                        - 0.22475541 * TEMP_AR_F * UMD_AR
                        - 6.83783 * POWER(10, -3) * POWER(TEMP_AR_F, 2)
                        - 5.481717 * POWER(10, -2) * POWER(UMD_AR, 2)
                        + 1.22874 * POWER(10, -3) * POWER(TEMP_AR_F, 2) * UMD_AR
                        + 8.5282 * POWER(10, -4) * TEMP_AR_F * POWER(UMD_AR, 2)
                        - 1.99 * POWER(10, -6) * POWER(TEMP_AR_F, 2) * POWER(UMD_AR, 2)
                        - (3.25 - 0.25 * UMD_AR) * ((17 - abs(TEMP_AR_F - 95)) / 17) * 0.5
                    )
                WHEN (TEMP_AR_F >= 80 AND TEMP_AR_F <= 87 AND UMD_AR > 85)
                    THEN (
                        - 42.379 + 2.04901523 * TEMP_AR_F + 10.14333127 * UMD_AR
                        - 0.22475541 * TEMP_AR_F * UMD_AR
                        - 6.83783 * POWER(10, -3) * POWER(TEMP_AR_F, 2)
                        - 5.481717 * POWER(10, -2) * POWER(UMD_AR, 2)
                        + 1.22874 * POWER(10, -3) * POWER(TEMP_AR_F, 2) * UMD_AR
                        + 8.5282 * POWER(10, -4) * TEMP_AR_F * POWER(UMD_AR, 2)
                        - 1.99 * POWER(10, -6) * POWER(TEMP_AR_F, 2) * POWER(UMD_AR, 2)
                        + 0.02 * (UMD_AR - 85) * (87 - TEMP_AR_F)
                    )
            ELSE (
                - 42.379 + 2.04901523 * TEMP_AR_F + 10.14333127 * UMD_AR
                - 0.22475541 * TEMP_AR_F * UMD_AR
                - 6.83783 * POWER(10, -3) * POWER(TEMP_AR_F, 2)
                - 5.481717 * POWER(10, -2) * POWER(UMD_AR, 2)
                + 1.22874 * POWER(10, -3) * POWER(TEMP_AR_F, 2) * UMD_AR
                + 8.5282 * POWER(10, -4) * TEMP_AR_F * POWER(UMD_AR, 2)
                - 1.99 * POWER(10, -6) * POWER(TEMP_AR_F, 2) * POWER(UMD_AR, 2)
            )
            END
        FROM "STATE_STREAM_1"
;

CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" (
    CIDADE VARCHAR(16),
    TEMP_AR FLOAT,
    UMD_AR INTEGER,
    INDICE_CALOR NUMERIC(10,2),
    NIVEL_ALERTA VARCHAR(16)
);

CREATE OR REPLACE PUMP "STREAM_PUMP" AS
    INSERT INTO "DESTINATION_SQL_STREAM"
        SELECT STREAM
            CIDADE,
            TEMP_AR_C,
            UMD_AR,
            ((INDICE_CALOR_F - 32) * 0.555555556),
            CASE
                WHEN ((INDICE_CALOR_F - 32) * 0.555555556) <= 27
                    THEN 'Normal'
                WHEN ((INDICE_CALOR_F - 32) * 0.555555556) <= 32
                    THEN 'Cautela'
                WHEN ((INDICE_CALOR_F - 32) * 0.555555556) <= 41
                    THEN 'Cautela Extrema'
                WHEN ((INDICE_CALOR_F - 32) * 0.555555556) <= 54
                    THEN 'Perigo'
                WHEN ((INDICE_CALOR_F - 32) * 0.555555556) > 54
                    THEN 'Perigo Extremo'
            END
        FROM "STATE_STREAM_2"
;
