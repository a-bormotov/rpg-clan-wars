WITH input(userId, subtract) AS (
  VALUES (35430, 80), (34478, 60), (35124, 60), (58157, 60), (34578, 60), (36776, 40), (37062, 40)
)
SELECT
  i.userId,
  COALESCE(ur.amount, 0) AS current_amount,
  i.subtract              AS subtract_amount,
  GREATEST(COALESCE(ur.amount, 0) - i.subtract, 0) AS result_amount
FROM input i
LEFT JOIN users_resources ur
  ON ur."userId" = i.userId
  AND ur."resourceType" = 'vSleep'
ORDER BY i.userId;
