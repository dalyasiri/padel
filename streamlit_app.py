import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

session = get_active_session()
st.set_page_config(layout="wide")


# Load player list BEFORE the form
players_df = session.sql("SELECT PLAYER_ID, PLAYER_NAME FROM MATCHES.PLAYERS ORDER BY PLAYER_NAME").to_pandas()
player_names = players_df["PLAYER_NAME"].tolist()
name_to_id = dict(zip(players_df["PLAYER_NAME"], players_df["PLAYER_ID"]))


# === 1. Leaderboard Section ===
st.title("🎾 Padel Team Dashboard")

st.subheader("🏆 Player Leaderboard")
leaderboard_df = session.sql("""
SELECT 
    p.PLAYER_NAME,
    COALESCE(stats.WINS, 0) AS WINS,
    COALESCE(stats.LOSSES, 0) AS LOSSES,
    COALESCE(stats.ROUNDS_WON, 0) AS ROUNDS_WON,
    COALESCE(stats.ROUNDS_LOST, 0) AS ROUNDS_LOST
FROM MATCHES.PLAYERS p
LEFT JOIN (
    SELECT 
        gp.PLAYER_ID,
        SUM(
            CASE 
                WHEN gp.TEAM_NUMBER = 1 AND pg.TEAM1_SCORE > pg.TEAM2_SCORE THEN 1
                WHEN gp.TEAM_NUMBER = 2 AND pg.TEAM2_SCORE > pg.TEAM1_SCORE THEN 1
                ELSE 0
            END
        ) AS WINS,
        SUM(
            CASE 
                WHEN gp.TEAM_NUMBER = 1 AND pg.TEAM1_SCORE <= pg.TEAM2_SCORE THEN 1
                WHEN gp.TEAM_NUMBER = 2 AND pg.TEAM2_SCORE <= pg.TEAM1_SCORE THEN 1
                ELSE 0
            END
        ) AS LOSSES,
        SUM(
            CASE 
                WHEN gp.TEAM_NUMBER = 1 THEN pg.TEAM1_SCORE
                WHEN gp.TEAM_NUMBER = 2 THEN pg.TEAM2_SCORE
            END
        ) AS ROUNDS_WON,
        SUM(
            CASE 
                WHEN gp.TEAM_NUMBER = 1 THEN pg.TEAM2_SCORE
                WHEN gp.TEAM_NUMBER = 2 THEN pg.TEAM1_SCORE
            END
        ) AS ROUNDS_LOST
    FROM MATCHES.GAME_PARTICIPANTS gp
    JOIN MATCHES.PADEL_GAMES pg ON gp.GAME_ID = pg.GAME_ID
    GROUP BY gp.PLAYER_ID
) stats ON p.PLAYER_ID = stats.PLAYER_ID
ORDER BY WINS DESC, ROUNDS_WON DESC, p.PLAYER_NAME;
""").to_pandas()
leaderboard_df.index = leaderboard_df.index + 1
st.dataframe(leaderboard_df, use_container_width=True)

# === 2. Input Game & Add Player Side-by-Side ===
# 🧱 Layout: Match Form (left) + Add Player (right)
col1, col2 = st.columns(2)

# === LEFT: Record a Match ===
with col1:
    st.subheader("📥 Record a New Match")

    # Load players
    players_df = session.sql("SELECT PLAYER_ID, PLAYER_NAME FROM MATCHES.PLAYERS ORDER BY PLAYER_NAME").to_pandas()
    player_names = players_df["PLAYER_NAME"].tolist()
    name_to_id = dict(zip(players_df["PLAYER_NAME"], players_df["PLAYER_ID"]))

    with st.form("match_form"):
        c1, c2 = st.columns(2)
        with c1:
            p1 = st.selectbox("Team 1 - Player 1", player_names, key="p1")
            p2 = st.selectbox("Team 1 - Player 2", player_names, key="p2")
        with c2:
            p3 = st.selectbox("Team 2 - Player 1", player_names, key="p3")
            p4 = st.selectbox("Team 2 - Player 2", player_names, key="p4")

        score1 = st.number_input("Team 1 Score", min_value=0, max_value=99, value=6)
        score2 = st.number_input("Team 2 Score", min_value=0, max_value=99, value=4)
        location = st.text_input("Location", "Dubai Hills")
        match_date = st.date_input("Match Date")

        submitted = st.form_submit_button("Submit Match")

        if submitted:
            selected_players = [p1, p2, p3, p4]
            if len(set(selected_players)) < 4:
                st.warning("⚠️ A player cannot be selected in multiple positions.")
            else:
                session.sql(f"""
                    INSERT INTO MATCHES.PADEL_GAMES (GAME_DATE, LOCATION, TEAM1_SCORE, TEAM2_SCORE)
                    VALUES (DATE('{match_date}'), '{location}', {score1}, {score2})
                """).collect()

                game_id = session.sql("SELECT MAX(GAME_ID) AS ID FROM MATCHES.PADEL_GAMES").to_pandas()["ID"].iloc[0]

                inserts = [
                    (game_id, name_to_id[p1], 1),
                    (game_id, name_to_id[p2], 1),
                    (game_id, name_to_id[p3], 2),
                    (game_id, name_to_id[p4], 2),
                ]

                for g_id, p_id, team in inserts:
                    session.sql(f"""
                        INSERT INTO MATCHES.GAME_PARTICIPANTS (GAME_ID, PLAYER_ID, TEAM_NUMBER)
                        VALUES ({g_id}, {p_id}, {team})
                    """).collect()

                st.success(f"✅ Game #{game_id} recorded successfully!")

# === RIGHT: Add New Player ===
with col2:
    st.subheader("➕ Add New Player")
    new_player = st.text_input("Enter player name")

    if st.button("Add Player"):
        if new_player.strip():
            session.sql(f"""
                INSERT INTO MATCHES.PLAYERS (PLAYER_NAME)
                VALUES ('{new_player.strip()}')
            """).collect()
            st.success(f"✅ Player '{new_player.strip()}' added!")
        else:
            st.warning("Please enter a valid name.")

# === 3. Head-to-Head Heatmap ===
st.subheader("🤼 Head-to-Head Win Heatmap")
h2h_df = session.sql("""
    WITH pairs AS (
        SELECT 
            a.PLAYER_ID AS PLAYER_A,
            b.PLAYER_ID AS PLAYER_B,
            a.GAME_ID,
            a.TEAM_NUMBER AS TEAM_A,
            b.TEAM_NUMBER AS TEAM_B,
            pg.TEAM1_SCORE,
            pg.TEAM2_SCORE
        FROM MATCHES.GAME_PARTICIPANTS a
        JOIN MATCHES.GAME_PARTICIPANTS b ON a.GAME_ID = b.GAME_ID AND a.PLAYER_ID != b.PLAYER_ID
        JOIN MATCHES.PADEL_GAMES pg ON a.GAME_ID = pg.GAME_ID
        WHERE a.TEAM_NUMBER != b.TEAM_NUMBER
    ),
    stats AS (
        SELECT 
            pa.PLAYER_NAME AS PLAYER_A,
            pb.PLAYER_NAME AS PLAYER_B,
            COUNT(*) AS GAMES_PLAYED,
            SUM(
                CASE 
                    WHEN (TEAM_A = 1 AND TEAM1_SCORE > TEAM2_SCORE) OR 
                         (TEAM_A = 2 AND TEAM2_SCORE > TEAM1_SCORE)
                    THEN 1 ELSE 0
                END
            ) AS WINS,
            SUM(
                CASE 
                    WHEN (TEAM_A = 1 AND TEAM1_SCORE < TEAM2_SCORE) OR 
                         (TEAM_A = 2 AND TEAM2_SCORE < TEAM1_SCORE)
                    THEN 1 ELSE 0
                END
            ) AS LOSSES
        FROM pairs
        JOIN MATCHES.PLAYERS pa ON pa.PLAYER_ID = pairs.PLAYER_A
        JOIN MATCHES.PLAYERS pb ON pb.PLAYER_ID = pairs.PLAYER_B
        GROUP BY pa.PLAYER_NAME, pb.PLAYER_NAME
    )
    SELECT * FROM stats
""").to_pandas()


# Filter out self-vs-self
h2h_filtered = h2h_df[h2h_df["PLAYER_A"] != h2h_df["PLAYER_B"]]

# Create summary string for hover
h2h_filtered["tooltip"] = (
    "Games: " + h2h_filtered["GAMES_PLAYED"].astype(str) +
    "\nWins: " + h2h_filtered["WINS"].astype(str) +
    "\nLosses: " + h2h_filtered["LOSSES"].astype(str)
)

heatmap = alt.Chart(h2h_filtered).mark_rect().encode(
    x=alt.X("PLAYER_B:O", title="Opponent"),
    y=alt.Y("PLAYER_A:O", title="Player"),
    color=alt.Color("WINS:Q", scale=alt.Scale(scheme="blues")),
    tooltip=["PLAYER_A", "PLAYER_B", "GAMES_PLAYED", "WINS", "LOSSES"]
).properties(
    width=600,
    height=600
)


st.altair_chart(heatmap, use_container_width=True)


# === 4. Bottom Charts Side-by-Side ===
st.subheader("📊 Game Stats")

col3, col4 = st.columns(2)

# Games Over Time (left)
with col3:
    st.markdown("**Games Played Over Time**")
    timeseries_df = session.sql("""
        SELECT GAME_DATE, COUNT(*) AS NUM_GAMES
        FROM MATCHES.PADEL_GAMES
        GROUP BY GAME_DATE
        ORDER BY GAME_DATE
    """).to_pandas()
    chart = alt.Chart(timeseries_df).mark_line(point=True).encode(
        x='GAME_DATE:T',
        y='NUM_GAMES:Q'
    ).properties(height=300)
    st.altair_chart(chart, use_container_width=True)

# Games by Location (right)
with col4:
    st.markdown("**Games by Location**")
    location_df = session.sql("""
        SELECT LOCATION, COUNT(*) AS NUM_GAMES
        FROM MATCHES.PADEL_GAMES
        GROUP BY LOCATION
        ORDER BY NUM_GAMES DESC
    """).to_pandas()
    bar = alt.Chart(location_df).mark_bar().encode(
        x=alt.X('NUM_GAMES:Q'),
        y=alt.Y('LOCATION:N', sort='-x')
    ).properties(height=300)
    st.altair_chart(bar, use_container_width=True)
