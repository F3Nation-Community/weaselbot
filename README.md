# Weaselbot

A supplemental tool for F3 regions to get more out of their data! Weaselbot is a Slack bot designed to get the PAX in your region more engaged, here's what he can do:

- **Achievements:** Incorporate automated "achievements" that are awarded based on F3 activity
- **Kotter Reports:** Detect when guys are starting to fall off the F3 wagon and send a notice to your site Qs
- [Coming Soon] archive your pictures uploaded to slack onto Google Drive and / or Google Photos

Please note - the first two features rely on data from PAXMiner. If you haven't already installed that and incorporated into your region, do that first and then come back here!

## Primary Setup

You will be creating the structure for Weaselbot, but I will run it from my end (similar to PAXMiner). There are some steps you need to take to set it up, and then some information I will need from you to add your instance to my daily runs.

### App setup

1. Navigate to [api.slack.com](), click on `Create an app`
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

![weaselbot-avatar](https://raw.githubusercontent.com/evanpetzoldt/weaselbot/master/readme_media/weaselbot_avatar.png)

5. Install the app to your region's workspace. Then, navigate to `Oauth & Permissions` on your bot page, and copy / save the `Bot User Auth Token`

### Additional Slack setup

1. If you want the achievements functionality, create a channel in your Slack workspace called `#achievements-unlocked`, or something similar. Copy that new channel's `Channel ID`. You can find this on Desktop by clicking on the dropdown for the channel, the channel's ID will be at the bottom
2. Please send me (@Moneyball, you can find me on the F3 Nation Slack space) the following info:
    * Your region's name and paxminer database name
    * The bot token that you generated above
    * The Channel ID that you generated in the previous step
    * Let me know if you want both the achievements and kotter report functionality
3. I will create some additional tables in your paxminer schema

## Achievements

If enabled, Weaselbot will pull your paxminer data to see if any of your PAX have crossed certain activity thresholds. If they have, and if they haven't already been awarded them (as per the `achievements_awarded` table), Weaselbot will give them a shout-out in your `#achievements-unlocked` channel and add the award to your `achievements_awarded` table. This table in turn drives the `achievements_view` view, which you can use in your region's dashboard / reporting for a cool "trophy case".

### Things to know / best practices

1. Weaselbot doesn't know what he doesn't know... If a tree falls in the woods (a backblast was not created or created incorrectly, guys not tagged etc), he doesn't know about it :)
2. Weaselbot works retroactively, pulling the full year's data every run. This means you could create / fix a backblast from a month ago and it would still be reflected. It also means that you could start using Weaselbot in the middle of the year, and he would award all achievements that should have been earned to date
3. By default, your region will have access to a dozen or so **automatic** achievements. These are achievements that Weaselbot will award on its own based on the paxminer data. If there's an achievement you don't like, you can simply delete it from your region's new `achievements_list` table
4. There is also functionality for **manual** achievements. These are achievements that you have to explicitly "tell" Weaselbot about. More info on that below
5. There are achievements that are specific to different types of activity. For example, if you track QSource events and attendance, there are achievements specific to that. The best way to have Weaselbot differentiate between your activity types is to name your backblast channels in the following way:
    * QSource: `#qsource`
    * Blackops (non-scheduled beatdowns): `#blackops`
    * Rucking: `#rucking`
    * Everything else is assumed to be a standard beatdown

### Manual achievements

You can add manual achievements in your region for things like "run a 5k" or "complete a GrowRuck GTE". Here are the steps to using this functionality:

1. Add records to your `achievements_list` table (follow the paxminer instructions for using DBeaver or something similar [link]). You will need to specificy the achievement's name, a description (for trophy cases, etc), a "verb" (essentially just the description in verb form) and a code to uniquely identify the achievement
2. **Important:** achievement codes cannot contain spaces - use underscores ( _ ) in their place
3. To tag guys for manual achievements, create a backblast. In the body of the backblast, create a separate line that says "Achievement: CODE_HERE", substituting in your manual achievement's code. See below for an example
4. **Note 1:** If you forget to add the "Achievement: " line or give the wrong code, paxminer will likely have already imported it if you try to edit the post. It's important to note that paxminer **does not update the backblast text in the database** if you edit a post, so you can't add in the manual achievement that way. The best way to handle is to edit the backblast in your paxminer data manually
5. **Note 2:** Posts with manual achievements will normally be counted as a beatdown for purposes of the automatic achievements. If you **don't** want it to, you can post the backblast in your `#achievements-unlocked` channel and Weaselbot will know not to include them


