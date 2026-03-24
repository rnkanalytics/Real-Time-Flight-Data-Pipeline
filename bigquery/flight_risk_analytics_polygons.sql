-- flight_risk_analytics.sql
-- This is the live view in BigQuery: flights-490708.flight_data.flight_risk_analytics
-- Uses real country border polygons (ST_CONTAINS) to eliminate bounding box false positives
-- Last updated: 2026-03-24

CREATE OR REPLACE VIEW `flights-490708.flight_data.flight_risk_analytics` AS

SELECT
  icao24,
  callsign,
  latitude,
  longitude,
  altitude,
  heading,
  velocity,
  vertical_rate,
  created_at,
  restricted_zone,
  severity,
  reason,
  miles_from_zone,
  zone_status,
  flight_phase,
  risk_score,
  speed_category,
  altitude_category,
  heading_direction,
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
    f.country        AS restricted_zone,
    f.severity,
    f.reason,
    f.dist_miles     AS miles_from_zone,
    f.zone_area,

    CASE
      WHEN f.dist_miles = 0    THEN 'INSIDE'
      WHEN f.dist_miles <= 69  THEN 'NEAR'
      WHEN f.dist_miles <= 138 THEN 'APPROACHING'
    END AS zone_status,

    CASE
      WHEN f.vertical_rate > 500  THEN 'CLIMBING'
      WHEN f.vertical_rate < -500 THEN 'DESCENDING'
      ELSE 'CRUISING'
    END AS flight_phase,

    CASE
      WHEN f.dist_miles = 0
      THEN CASE f.severity
        WHEN 'CLOSED'     THEN 10
        WHEN 'HIGH RISK'  THEN 7
        WHEN 'RESTRICTED' THEN 5
        ELSE 3
      END
      WHEN f.dist_miles <= 69
      THEN CASE f.severity
        WHEN 'CLOSED'     THEN 7
        WHEN 'HIGH RISK'  THEN 5
        WHEN 'RESTRICTED' THEN 3
        ELSE 2
      END
      ELSE 2
    END AS risk_score,

    CASE
      WHEN f.velocity > 500 THEN 'HIGH SPEED'
      WHEN f.velocity > 300 THEN 'CRUISE SPEED'
      WHEN f.velocity > 100 THEN 'LOW SPEED'
      ELSE 'SLOW'
    END AS speed_category,

    CASE
      WHEN f.altitude > 35000 THEN 'HIGH ALTITUDE'
      WHEN f.altitude > 20000 THEN 'MID ALTITUDE'
      WHEN f.altitude > 10000 THEN 'LOW ALTITUDE'
      ELSE 'VERY LOW'
    END AS altitude_category,

    CASE
      WHEN f.heading BETWEEN 315 AND 360 OR f.heading BETWEEN 0 AND 45 THEN 'NORTH'
      WHEN f.heading BETWEEN 45  AND 135 THEN 'EAST'
      WHEN f.heading BETWEEN 135 AND 225 THEN 'SOUTH'
      WHEN f.heading BETWEEN 225 AND 315 THEN 'WEST'
    END AS heading_direction

  FROM (
    SELECT
      fl.*,
      r.country,
      r.severity,
      r.reason,
      (r.max_lat - r.min_lat) * (r.max_lon - r.min_lon) AS zone_area,

      CASE
        -- Real polygon exists AND flight is inside it → truly inside
        WHEN cb.border IS NOT NULL
          AND ST_CONTAINS(cb.border, ST_GEOGPOINT(fl.longitude, fl.latitude))
        THEN 0.0

        -- Real polygon exists BUT flight is outside it → not inside this zone
        -- Set to 999 so it is excluded by the WHERE clause below
        WHEN cb.border IS NOT NULL
          AND NOT ST_CONTAINS(cb.border, ST_GEOGPOINT(fl.longitude, fl.latitude))
        THEN 999.0

        -- No polygon available → fall back to bounding box distance
        ELSE ROUND(
          ST_DISTANCE(
            ST_GEOGPOINT(fl.longitude, fl.latitude),
            ST_CLOSESTPOINT(
              ST_MAKEPOLYGON(ST_MAKELINE([
                ST_GEOGPOINT(r.min_lon, r.min_lat),
                ST_GEOGPOINT(r.max_lon, r.min_lat),
                ST_GEOGPOINT(r.max_lon, r.max_lat),
                ST_GEOGPOINT(r.min_lon, r.max_lat),
                ST_GEOGPOINT(r.min_lon, r.min_lat)
              ])),
              ST_GEOGPOINT(fl.longitude, fl.latitude)
            )
          ) * 0.000621371
        , 1)
      END AS dist_miles

    FROM `flights-490708.flight_data.flights` fl
    CROSS JOIN `flights-490708.flight_data.restricted_airspace` r
    LEFT JOIN `flights-490708.flight_data.country_borders` cb
      ON LOWER(cb.country_name) = LOWER(r.country)

    WHERE
      fl.latitude    IS NOT NULL
      AND fl.longitude   IS NOT NULL
      AND fl.altitude    IS NOT NULL
      AND fl.velocity    IS NOT NULL
      AND fl.heading     IS NOT NULL
      AND fl.altitude    > 1000
      AND fl.velocity    > 50
      AND fl.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 MINUTE)
      AND fl.latitude  BETWEEN r.min_lat - 2 AND r.max_lat + 2
      AND fl.longitude BETWEEN r.min_lon - 2 AND r.max_lon + 2
  ) f
  WHERE f.dist_miles < 999
)

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY icao24
  ORDER BY risk_score DESC, miles_from_zone ASC, zone_area ASC
) = 1