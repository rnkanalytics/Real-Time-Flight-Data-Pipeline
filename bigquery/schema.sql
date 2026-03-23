-- ============================================
-- REAL-TIME FLIGHT RISK MONITOR
-- BigQuery Schema
-- Project: flights-490708
-- Dataset: flight_data
-- ============================================


-- --------------------------------------------
-- TABLE 1: flights
-- Raw ADS-B streaming data from airplanes.live
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS `flights-490708.flight_data.flights` (
  icao24        STRING,
  callsign      STRING,
  latitude      FLOAT64,
  longitude     FLOAT64,
  altitude      FLOAT64,
  heading       FLOAT64,
  velocity      FLOAT64,
  vertical_rate FLOAT64,
  timestamp     STRING,
  created_at    TIMESTAMP
)
PARTITION BY DATE(created_at)
OPTIONS (partition_expiration_days = 1);


-- --------------------------------------------
-- TABLE 2: restricted_airspace
-- AI-updated daily via Claude + GitHub Actions
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS `flights-490708.flight_data.restricted_airspace` (
  country    STRING,
  reason     STRING,
  min_lat    FLOAT64,
  max_lat    FLOAT64,
  min_lon    FLOAT64,
  max_lon    FLOAT64,
  severity   STRING,
  since      DATE,
  updated_at TIMESTAMP
);


-- --------------------------------------------
-- TABLE 3: flight_risk_snapshot
-- Persistent last known state for dashboard
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS `flights-490708.flight_data.flight_risk_snapshot` (
  icao24            STRING,
  callsign          STRING,
  latitude          FLOAT64,
  longitude         FLOAT64,
  altitude          FLOAT64,
  heading           FLOAT64,
  velocity          FLOAT64,
  vertical_rate     FLOAT64,
  restricted_zone   STRING,
  severity          STRING,
  reason            STRING,
  miles_from_zone   FLOAT64,
  zone_status       STRING,
  flight_phase      STRING,
  risk_score        INT64,
  speed_category    STRING,
  altitude_category STRING,
  heading_direction STRING,
  risk_label        STRING,
  snapshot_time     TIMESTAMP
)
PARTITION BY DATE(snapshot_time);


-- --------------------------------------------
-- VIEW: flight_risk_analytics
-- JOIN flights + restricted_airspace
-- Risk scoring, distance calculation, enrichment
-- --------------------------------------------
CREATE OR REPLACE VIEW `flights-490708.flight_data.flight_risk_analytics` AS

SELECT
  *,
  CASE
    WHEN risk_score >= 8 THEN '🔴 CRITICAL'
    WHEN risk_score >= 5 THEN '🟠 HIGH'
    WHEN risk_score >= 3 THEN '🟡 MEDIUM'
    ELSE '🟢 LOW'
  END AS risk_label

FROM (
  SELECT
    f.icao24,
    f.callsign,
    f.latitude,
    f.longitude,
    f.altitude,
    f.heading,
    f.velocity,
    f.vertical_rate,
    f.created_at,
    r.country AS restricted_zone,
    r.severity,
    r.reason,

    -- Distance in miles from restricted zone boundary
    ROUND(
      ST_DISTANCE(
        ST_GEOGPOINT(f.longitude, f.latitude),
        ST_CLOSESTPOINT(
          ST_MAKEPOLYGON(
            ST_MAKELINE([
              ST_GEOGPOINT(r.min_lon, r.min_lat),
              ST_GEOGPOINT(r.max_lon, r.min_lat),
              ST_GEOGPOINT(r.max_lon, r.max_lat),
              ST_GEOGPOINT(r.min_lon, r.max_lat),
              ST_GEOGPOINT(r.min_lon, r.min_lat)
            ])
          ),
          ST_GEOGPOINT(f.longitude, f.latitude)
        )
      ) * 0.000621371
    , 1) AS miles_from_zone,

    -- Zone status based on distance
    CASE
      WHEN ST_DISTANCE(
        ST_GEOGPOINT(f.longitude, f.latitude),
        ST_CLOSESTPOINT(
          ST_MAKEPOLYGON(ST_MAKELINE([
            ST_GEOGPOINT(r.min_lon, r.min_lat),
            ST_GEOGPOINT(r.max_lon, r.min_lat),
            ST_GEOGPOINT(r.max_lon, r.max_lat),
            ST_GEOGPOINT(r.min_lon, r.max_lat),
            ST_GEOGPOINT(r.min_lon, r.min_lat)
          ])),
          ST_GEOGPOINT(f.longitude, f.latitude)
        )
      ) * 0.000621371 = 0    THEN 'INSIDE'
      WHEN ST_DISTANCE(
        ST_GEOGPOINT(f.longitude, f.latitude),
        ST_CLOSESTPOINT(
          ST_MAKEPOLYGON(ST_MAKELINE([
            ST_GEOGPOINT(r.min_lon, r.min_lat),
            ST_GEOGPOINT(r.max_lon, r.min_lat),
            ST_GEOGPOINT(r.max_lon, r.max_lat),
            ST_GEOGPOINT(r.min_lon, r.max_lat),
            ST_GEOGPOINT(r.min_lon, r.min_lat)
          ])),
          ST_GEOGPOINT(f.longitude, f.latitude)
        )
      ) * 0.000621371 <= 69  THEN 'NEAR'
      WHEN ST_DISTANCE(
        ST_GEOGPOINT(f.longitude, f.latitude),
        ST_CLOSESTPOINT(
          ST_MAKEPOLYGON(ST_MAKELINE([
            ST_GEOGPOINT(r.min_lon, r.min_lat),
            ST_GEOGPOINT(r.max_lon, r.min_lat),
            ST_GEOGPOINT(r.max_lon, r.max_lat),
            ST_GEOGPOINT(r.min_lon, r.max_lat),
            ST_GEOGPOINT(r.min_lon, r.min_lat)
          ])),
          ST_GEOGPOINT(f.longitude, f.latitude)
        )
      ) * 0.000621371 <= 138 THEN 'APPROACHING'
    END AS zone_status,

    -- Flight phase
    CASE
      WHEN f.vertical_rate > 500  THEN 'CLIMBING'
      WHEN f.vertical_rate < -500 THEN 'DESCENDING'
      ELSE 'CRUISING'
    END AS flight_phase,

    -- Risk score
    CASE
      WHEN ST_DISTANCE(
        ST_GEOGPOINT(f.longitude, f.latitude),
        ST_CLOSESTPOINT(
          ST_MAKEPOLYGON(ST_MAKELINE([
            ST_GEOGPOINT(r.min_lon, r.min_lat),
            ST_GEOGPOINT(r.max_lon, r.min_lat),
            ST_GEOGPOINT(r.max_lon, r.max_lat),
            ST_GEOGPOINT(r.min_lon, r.max_lat),
            ST_GEOGPOINT(r.min_lon, r.min_lat)
          ])),
          ST_GEOGPOINT(f.longitude, f.latitude)
        )
      ) * 0.000621371 = 0
      THEN
        CASE r.severity
          WHEN 'CLOSED'     THEN 10
          WHEN 'HIGH RISK'  THEN 7
          WHEN 'RESTRICTED' THEN 5
          ELSE 3
        END
      WHEN ST_DISTANCE(
        ST_GEOGPOINT(f.longitude, f.latitude),
        ST_CLOSESTPOINT(
          ST_MAKEPOLYGON(ST_MAKELINE([
            ST_GEOGPOINT(r.min_lon, r.min_lat),
            ST_GEOGPOINT(r.max_lon, r.min_lat),
            ST_GEOGPOINT(r.max_lon, r.max_lat),
            ST_GEOGPOINT(r.min_lon, r.max_lat),
            ST_GEOGPOINT(r.min_lon, r.min_lat)
          ])),
          ST_GEOGPOINT(f.longitude, f.latitude)
        )
      ) * 0.000621371 <= 69
      THEN
        CASE r.severity
          WHEN 'CLOSED'     THEN 7
          WHEN 'HIGH RISK'  THEN 5
          WHEN 'RESTRICTED' THEN 3
          ELSE 2
        END
      ELSE 2
    END AS risk_score,

    -- Speed category
    CASE
      WHEN f.velocity > 500 THEN 'HIGH SPEED'
      WHEN f.velocity > 300 THEN 'CRUISE SPEED'
      WHEN f.velocity > 100 THEN 'LOW SPEED'
      ELSE 'SLOW'
    END AS speed_category,

    -- Altitude category
    CASE
      WHEN f.altitude > 35000 THEN 'HIGH ALTITUDE'
      WHEN f.altitude > 20000 THEN 'MID ALTITUDE'
      WHEN f.altitude > 10000 THEN 'LOW ALTITUDE'
      ELSE 'VERY LOW'
    END AS altitude_category,

    -- Heading direction
    CASE
      WHEN f.heading BETWEEN 315 AND 360 OR f.heading BETWEEN 0 AND 45 THEN 'NORTH'
      WHEN f.heading BETWEEN 45  AND 135 THEN 'EAST'
      WHEN f.heading BETWEEN 135 AND 225 THEN 'SOUTH'
      WHEN f.heading BETWEEN 225 AND 315 THEN 'WEST'
    END AS heading_direction

  FROM `flights-490708.flight_data.flights` f
  CROSS JOIN `flights-490708.flight_data.restricted_airspace` r

  WHERE
    f.latitude    IS NOT NULL
    AND f.longitude   IS NOT NULL
    AND f.altitude    IS NOT NULL
    AND f.velocity    IS NOT NULL
    AND f.heading     IS NOT NULL
    AND f.altitude    > 1000
    AND f.velocity    > 50
    AND f.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 MINUTE)
    AND (
      f.latitude  BETWEEN r.min_lat - 2 AND r.max_lat + 2
      AND f.longitude BETWEEN r.min_lon - 2 AND r.max_lon + 2
    )
    AND NOT (
      r.country = 'Ukraine'
      AND f.latitude < 45.5
    )
)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY icao24
  ORDER BY risk_score DESC, miles_from_zone ASC
) = 1;
