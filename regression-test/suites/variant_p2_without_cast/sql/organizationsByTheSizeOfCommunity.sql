SELECT
    lower(split_part(repo_name, '/', 1)) AS org,
    count(distinct actor_login) AS authors,
    count(distinct pr_author) AS pr_authors,
    count(distinct issue_author) AS issue_authors,
    count(distinct comment_author) AS comment_authors,
    count(distinct review_author) AS review_authors,
    count(distinct push_author) AS push_authors
FROM
(
SELECT
    repo:name as repo_name,
    actor:login  as actor_login,
    CASE WHEN type = 'PullRequestEvent' THEN actor:login  ELSE NULL END pr_author,
    CASE WHEN type = 'IssuesEvent' THEN actor:login  ELSE NULL END issue_author,
    CASE WHEN type = 'IssueCommentEvent' THEN actor:login  ELSE NULL END comment_author,
    CASE WHEN type = 'PullRequestReviewCommentEvent' THEN actor:login  ELSE NULL END review_author,
    CASE WHEN type = 'PushEvent' THEN actor:login  ELSE NULL END push_author
FROM github_events
WHERE type IN ('PullRequestEvent', 'IssuesEvent', 'IssueCommentEvent', 'PullRequestReviewCommentEvent', 'PushEvent')
) t
GROUP BY org
ORDER BY authors DESC, org
limit 5
