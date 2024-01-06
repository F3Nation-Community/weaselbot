# WeaselBot

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)](https://python-poetry.org/)

A supplemental tool for [F3 regions](https://f3nation.com/) to get more out of their data! WeaselBot is a Slack bot designed to get the PAX in your region more engaged, here's what he can do:

- [Achievements](#achievements): Incorporate automated "achievements" that are awarded based on F3 activity

<img src="https://raw.githubusercontent.com/evanpetzoldt/weaselbot/master/readme_media/achievement-screenshot.png" width="300" />

- [Kotter Reports](#kotter-reports): Detect when guys are starting to fall off the F3 wagon and send a notice to your site Qs

<img src="https://raw.githubusercontent.com/evanpetzoldt/weaselbot/master/readme_media/kotter-report.png" width="300" />

- [Coming Soon] archive your pictures uploaded to slack onto Google Drive and / or Google Photos

Please note - the first two features rely on data from [PAXMiner](https://f3stlouis.com/paxminer/). If you haven't already installed that and incorporated into your region (which you should anyway!), [do that first](https://f3stlouis.com/paxminer-setup/) and then come back here!

## Primary Setup

You will be creating the structure for WeaselBot, but I will run it from my end (similar to PAXMiner). There are some steps you need to take to set it up, and then some information I will need from you to add your instance to my daily runs.

### App setup

1. Navigate to [api.slack.com](https://api.slack.com/), click on `Create an app`
2. Select `From an app manifest`, and select your region's workspace
3. Paste in the following code, then click `Create`:
```yaml
display_information:
  name: WeaselBot
  description: "Hi there! I'm a bot created by @Moneyball in F3 St. Charles to do some stuff like #achievement-unlocked."
  background_color: "#2c2d30"
features:
  bot_user:
    display_name: WeaselBot
    always_online: true
oauth_config:
  scopes:
    bot:
      - app_mentions:read
      - channels:join
      - channels:read
      - chat:write
      - files:read
      - files:write
      - im:read
      - reactions:write
      - users:read
      - channels:history
settings:
  org_deploy_enabled: false
  socket_mode_enabled: false
  token_rotation_enabled: false
```
4. If desired, you can use my DALL-E generated icon for its avatar (you can set this at the bottom of `Basic Information`):

<img src="https://raw.githubusercontent.com/evanpetzoldt/weaselbot/master/readme_media/weaselbot_avatar.png" width="200" />


5. Install the app to your region's workspace. Then, navigate to `Oauth & Permissions` on your bot page, and copy / save the `Bot User Auth Token`

### Additional Slack setup

1. If you want the achievements functionality, create a channel in your Slack workspace called `#achievement-unlocked`, or something similar. Copy that new channel's `Channel ID`. You can find this on Desktop by clicking on the dropdown for the channel, the channel's ID will be at the bottom
2. Please send me (@Moneyball, you can find me on the F3 Nation Slack space) the following info:
    * Your region's name and PAXMiner database name
    * The bot token that you generated above
    * The Channel ID that you generated in the previous step for achievements
    * The User or Channel ID of where you want the FULL kotter report going to... many regions have been using private channels for this. If using a private channel, you will need to add Weaselbot to the channel manually
    * Let me know if you want both the achievements and kotter report functionality
3. I will create some additional tables in your PAXMiner schema

## Achievements

If enabled, WeaselBot will pull your PAXMiner data daily to see if any of your PAX have crossed certain activity thresholds. If they have, and if they haven't already been awarded them (as per the `achievements_awarded` table), WeaselBot will give them a shout-out in your `#achievement-unlocked` channel and add the award to your `achievements_awarded` table. This table in turn drives the `achievements_view` view, which you can use in your region's dashboard / reporting for a cool "trophy case".

### Things to know / best practices

1. WeaselBot doesn't know what he doesn't know... If a tree falls in the woods (a backblast was not created or created incorrectly, guys not tagged etc), he doesn't know about it :) While I'm happy to investigate issues with WeaselBot, I won't be able to support every region's request of "why didn't this guy get this achievement?", as 99% of the time it's likely a data entry error
2. WeaselBot works retroactively, pulling the full year's data every run. This means you could create / fix a backblast from a month ago and it would still be reflected. It also means that you could start using WeaselBot in the middle of the year, and he would award all achievements that should have been earned to date
3. By default, your region will have access to a dozen or so **automatic** achievements. These are achievements that WeaselBot will award on its own based on the paxminer data. If there's an achievement you don't like, you can simply delete it from your region's new `achievements_list` table
4. There is also functionality for **manual** achievements. These are achievements that you have to explicitly "tell" WeaselBot about. More info on that below
5. There are achievements that are specific to different types of activity. For example, if you track QSource events and attendance, there are achievements specific to that. The best way to have WeaselBot differentiate between your activity types is to name your backblast channels in the following way:
    * QSource: `#qsource`
    * Blackops (non-scheduled beatdowns): `#blackops`
    * Rucking: `#rucking`
    * Everything else is assumed to be a standard beatdown
6. Weaselbot now uses Nation-level data for both kotter reports and achievements. This means that guys posting DR will get credit for those posts, WITHOUT making special DR posts. For this reason, I recommend against making DR posts unless the DR region does not use PAXMiner.

### Manual achievements

You can add manual achievements in your region for things like "run a 5k" or "complete a GrowRuck GTE". Here are the steps to using this functionality:

1. Add records to your `achievements_list` table (follow the [PAXMiner instructions for using DBeaver](https://f3stlouis.com/paxminer-setup/) or something similar). You will need to specify the achievement's name, a description (for trophy cases, etc), a "verb" (essentially just the description in verb form) and a code to uniquely identify the achievement
2. **Important:** achievement codes cannot contain spaces - use underscores ( _ ) in their place
3. To tag guys for manual achievements, create a backblast. In the body of the backblast, create a separate line that says "Achievement: CODE_HERE", substituting in your manual achievement's code. See below for an example

<img src="https://raw.githubusercontent.com/evanpetzoldt/weaselbot/master/readme_media/example-manual-achievement.png" width="500" />

4. **Note 1:** If you forget to add the "Achievement: " line or give the wrong code, PAXMiner will likely have already imported it if you try to edit the post. It's important to note that PAXMiner **does not update the backblast text in the database** if you edit a post, so you can't add in the manual achievement that way. The best way to handle is to edit the backblast in your PAXMiner data manually
5. **Note 2:** Posts with manual achievements will normally be counted as a beatdown for purposes of the automatic achievements. If you **don't** want it to, you can post the backblast in your `#achievement-unlocked` channel and WeaselBot will know not to include them

## Kotter reports

If enabled, once a week WeaselBot will check your PAXMiner data and see if any of your PAX have hit certain thresholds of inactivity - right now, that is set at 2 weeks of not having posted anywhere, but less than 4 weeks of not having posted anywhere (to prevent WeaselBot from including them for the rest of time). PAX are also tracked based on their last Q, and guys without Qs or a long time since will be included on the report.

WeaselBot compiles this list, then sends reports out. At a minimum, the full list will be sent to a user you specify. If desired, the list can also be aggregated by those PAX "home AO", as determined by their recent posting history. If you specify your Site Qs, those lists will be sent to them according to their AO.

### Setup

1. Send me the User ID of the HIM you want to get the full list. On desktop, you can get this by clicking on their profile, then hit the `...` and then `Copy Member ID` (you can also find these on your `users` table)
2. I will create a new column on your `aos` table for Site Qs
3. Enter your Site Qs' **user_ids** (not names) in this column. Update as necessary when Site Qs switch out

## Contributing

We're always happy to take PRs! Below are instructions for running Weaselbot locally for development.

1. Clone the repo:
```sh
git clone https://github.com/evanpetzoldt/weaselbot.git
cd weaselbot
```
2. Install python poetry: https://python-poetry.org/docs/
3. Create virtual environment and install project dependencies:
```sh
poetry env use /path/to/python3.11
poetry install
```
4. If using the paxminer database (most common use), copy `.env.example`, rename to `.env` and fill credentials (ask Moneyball for these). Otherwise, create your own local db and use your own credentials (initialization scripts coming soon).
5. Run scripts with `poetry run python script_name.py`
6. This project uses Ruff / Black to apply consistent code formatting. Use `pre-commit install` to install the pre commit hooks (I'll eventually apply these as Github Actions on pushes to `main`)
7. (Coming soon): run unit tests through `poetry run pytest`, which automatically runs all tests in the `tests/` folder
