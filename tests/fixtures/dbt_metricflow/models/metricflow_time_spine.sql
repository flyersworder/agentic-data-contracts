select cast(range as date) as date_day
from range(date '2026-01-01', date '2026-12-31', interval 1 day)
