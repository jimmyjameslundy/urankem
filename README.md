# U-RankEm — private pool website

Python 3.10+ only. No pip packages.

## Run on your PC (for a test)

```
cd urankem-site
python server.py
```

Open http://127.0.0.1:8080

## Host for everyone (the live link)

Use a free host such as Render.

1. Put this `urankem-site` folder in a GitHub repository (or upload the zip).
2. Go to https://render.com and sign in with GitHub.
3. New → Web Service → that repo.
4. Runtime: Python. Start command: `python server.py`
5. Render sets `PORT` automatically. The server already reads it.
6. After deploy you get a URL like `https://u-rankem.onrender.com`
7. That is the dedicated site. Send it to friends.

Free Render apps sleep after idle time. The first open after a nap can take 30–60 seconds.

## How the game runs

1. Host opens the site → Create a pool (name + PIN) → gets a 6-character code and invite link `/?pool=CODE`.
2. Friends open the link → Join with their name and their own PIN.
3. Each player fills Week ballot → Save and submit.
4. Monday the host pastes the official AP Top 20 → Score this week.
5. Host copies the results text and sends it to the group. Standings also stay on the site (weekly winner + season leader).
