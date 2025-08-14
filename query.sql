WITH input("userId", subtract, clan) AS (
  VALUES
    (35430, 80, 'Wolves'),
    (34478, 60, 'Wolves'),
    (35124, 60, 'Foxes'),
    (58157, 60, 'Foxes'),
    (34578, 60, 'Wolves'),
    (36776, 40, 'Owls'),
    (37062, 40, 'Owls')
),
ur_agg AS (
  SELECT "userId", SUM(amount) AS amount
  FROM users_resources
  WHERE "resourceType" = 'vSleep'
  GROUP BY "userId"
),
player_stats AS (
  SELECT
    i.clan,
    GREATEST(COALESCE(u.amount, 0) - i.subtract, 0) AS result_amount
  FROM input i
  LEFT JOIN ur_agg u ON u."userId" = i."userId"
)
SELECT
  clan,
  SUM(result_amount) AS clan_result_total
FROM player_stats
GROUP BY clan
ORDER BY clan_result_total DESC, clan;
