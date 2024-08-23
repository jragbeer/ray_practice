from jragbeer_common import *
import ray
import cudf.pandas
# cudf.pandas.install()

# pandas settings for settingwarning
pd.options.mode.chained_assignment = None  # default='warn'
# pandas settings for terminal output
pd.set_option("display.width", 500)
pd.set_option("display.max_columns", None)
pd.set_option("display.float_format", lambda x: "%.5f" % x)

path = os.path.abspath(os.path.dirname(__file__)).replace("\\", "/") + "/"
today = datetime.datetime.now()


load_dotenv(path + '.env')
secrets = dict(dotenv_values(path + ".env"))

# logger
dagster_logger = logging.getLogger("logger")
dagster_logger.setLevel(logging.INFO)
# create console handler
handler = logging.StreamHandler()
# create formatter and add it to the handler
formatter = logging.Formatter('%(asctime)s, %(message)s')
handler.setFormatter(formatter)
# add the handler to the logger
dagster_logger.addHandler(handler)
# create console handler
handler2 = logging.FileHandler(path + "bowling_analysis.log")
# add the handler to the logger
dagster_logger.addHandler(handler2)
dagster_logger.info(today)
# code version
app_version_number = "0.1"
dagster_logger.info(f"Version:  {app_version_number}")

context = ray.init(
    address='local',
                   log_to_driver=False,
                   )
print(context)

# Change this to match your cluster scale.
# parameters for the simulation
high_score = 210
loops = 10_000
NUM_SAMPLING_TASKS = 200
NUM_SAMPLES_PER_TASK = 5
num_games_to_simulate = NUM_SAMPLING_TASKS * NUM_SAMPLES_PER_TASK

print("num_games_to_simulate per loop", num_games_to_simulate)
compare_column = 'ball_score'  # ball_score or pins_hit

# each pin for easier analysis, and it's # of points as the value
balls = {"PIN"+str(x): 1 for x in range(1, 11)}
base_games_by_score = {x:[] for x in range(301)}


def create_game_template() -> pd.DataFrame:
    # setting up the game totals dataframe
    game_output_df_template = pd.DataFrame({'a':range(1, 22)}) # max number of balls that can be thrown in 1 game
    game_output_df_template['frame'] = (game_output_df_template['a']%2 + game_output_df_template['a'])/2
    game_output_df_template['ball_in_frame'] = (game_output_df_template['a'] + 1)%2 + 1
    # last ball of the game, special case
    game_output_df_template.loc[max(game_output_df_template.index), 'frame'] = 10
    game_output_df_template.loc[max(game_output_df_template.index), 'ball_in_frame'] = 3
    game_output_df_template['frame'] = game_output_df_template['frame'].astype(int)
    return game_output_df_template.drop(columns=['a'])
def simulate_game(game_template_df: pd.DataFrame) -> pd.DataFrame:
    game_output_df_template = game_template_df.copy()
    # a list of the pins hit, each item in the list will be a Ball in a Frame
    ph = []
    for i in game_output_df_template.itertuples():
        # Frames 1-9 are similar cases
        # unique circumstances involving Frame 10: Ball 1 and Frame 10: Ball 2
        if i.ball_in_frame == 1:
            num_pins_down = np.random.randint(0,11)
            all_combos = list(itertools.combinations(balls.keys(), num_pins_down))
            # append the first out of the list of returned possible Balls in the Frame
            ph.append(all_combos[0])
        elif i.ball_in_frame == 2:
            # find which pins are hit and only look at the other ones
            list_of_pins_left = [kk for kk in balls.keys() if kk not in ph[len(ph)-1]]
            num_pins_down = np.random.randint(0, len(list_of_pins_left)+1)
            all_combos = list(itertools.combinations(list_of_pins_left, num_pins_down))
            # append the first out of the list of returned possible Balls in the Frame
            ph.append(all_combos[0])
        # Frame 10 is a unique case, 3 possbile balls in the frame
        if i.frame == 10:
            if i.ball_in_frame == 3:
                #  find the two most recent Balls in Frame 10
                last_ball = len(ph[len(ph)-1])
                last_last_ball = len(ph[len(ph)-2])
                #  find out if the third ball can be played (if a mark occurred in first two Balls)
                if last_ball + last_last_ball == 10:
                    num_pins_down = np.random.randint(0, 10)
                    all_combos = list(itertools.combinations(balls.keys(), num_pins_down))
                    ph.append(all_combos[0])
                # if no mark, last Ball can't be thrown
                else:
                    ph.append(tuple())

    game_output_df_template["pins_hit"] = ph
    game_output_df_template['ball_score'] = [len(i) for i in game_output_df_template['pins_hit']]
    return game_output_df_template.copy()
def find_marks(game_df: pd.DataFrame) -> pd.DataFrame:
    special_score = []
    for frame in sorted(game_df['frame'].unique()):
        tmp = game_df[game_df['frame'] == frame].copy()
        # special handling for the last ball of the game
        if frame == 10:
            # first ball of the frame
            if tmp[tmp['ball_in_frame'] == 1]['ball_score'].sum() == 10:
                special_score.append('STRIKE')
            else:
                special_score.append('')
            # second ball of the frame
            if tmp[tmp['ball_in_frame'] == 2]['ball_score'].sum() == 10:
                special_score.append('STRIKE')
            elif (tmp[tmp['ball_in_frame'] == 1]['ball_score'].sum() + tmp[tmp['ball_in_frame'] == 2]['ball_score'].sum() == 10):
                if (tmp[tmp['ball_in_frame'] == 2]['ball_score'].sum() != 10) and (tmp[tmp['ball_in_frame'] == 1]['ball_score'].sum() != 10):
                    special_score.append('SPARE')
                else:
                    special_score.append('')
            else:
                special_score.append('')
            # last ball of the frame
            if tmp[tmp['ball_in_frame'] == 3]['ball_score'].sum() == 10:
                special_score.append('STRIKE')
            elif (tmp[tmp['ball_in_frame'] == 3]['ball_score'].sum() + tmp[tmp['ball_in_frame'] == 2][
                'ball_score'].sum() == 10):
                if tmp[tmp['ball_in_frame'] == 3]['ball_score'].sum() != 10 and (tmp[tmp['ball_in_frame'] == 2]['ball_score'].sum() +tmp[tmp['ball_in_frame'] == 1]['ball_score'].sum() != 10) :
                    special_score.append('SPARE')
                else:
                    special_score.append('')
            else:
                special_score.append('')
        else:

            # if the score == 10 aka a MARK
            if tmp['ball_score'].sum() == 10:
                # of the first ball == 10 aka a STRIKE
                if tmp[tmp['ball_in_frame'] == 1]['ball_score'].sum() == 10:
                    special_score.append('STRIKE')
                    special_score.append('')
                elif tmp[tmp['ball_in_frame'] <= 2]['ball_score'].sum() == 10:

                    # if not just the first ball, it's a SPARE
                    special_score.append('')
                    special_score.append('SPARE')
            else:
                special_score.append('')
                special_score.append('')

    game_df['special'] = special_score
    return game_df.copy()
def get_scores(game_df: pd.DataFrame) -> pd.DataFrame:
    game_df['added_score1'] = 0
    game_df['added_score2'] = 0
    for i in game_df.itertuples():
        # if it's a spare, get the next ball and add it to the added_score1 column
        if i.special == 'SPARE':
            game_df.loc[i.Index, 'added_score1'] = game_df.loc[i.Index+1, 'ball_score']
            # game_df['added_score1'].iloc[i.Index] = game_df['ball_score'].iloc[i.Index+1]
        # if it's a strike, get the next ball and add it to the added_score1 column and add the next ball to
        # added_score2 column, handling for if there isn't a next score at later points in the game
        elif i.special == "STRIKE":
            try:
                next_valid_ball_offset = 2
                game_df.loc[i.Index, 'added_score1'] = game_df.loc[i.Index+next_valid_ball_offset, 'ball_score']
            except:
                pass
            try:
                next_next_valid_ball_offset = 3
                try:
                    if game_df.loc[i.Index + 2, 'special'] == 'STRIKE':
                        next_next_valid_ball_offset = next_next_valid_ball_offset + 1
                except:
                    pass
                game_df.loc[i.Index, 'added_score2'] = game_df.loc[i.Index+next_next_valid_ball_offset, 'ball_score']
            except:
                pass
    # calculate the ball by ball score and the total score
    game_df['total_ball_score'] = game_df['ball_score'] + game_df['added_score1'] + game_df['added_score2']
    game_df['running_score'] = game_df['total_ball_score'].cumsum()
    return game_df.copy()

# post-simulation stats funcs
def find_best_game(games: dict) -> tuple[int, int, int]:
    best_score = 0
    index_of_best_game = 0
    number_of_games_with_strike = 0
    game_with_a_strike = None
    for k,v in games.items():
        if not v.empty:
            tmp = v["running_score"].max()
            if tmp > best_score:
                best_score = tmp
                index_of_best_game = k
            if 'STRIKE' in [pp for pp in v['special'].tolist()]:
                number_of_games_with_strike = number_of_games_with_strike + 1
                game_with_a_strike = k
    return index_of_best_game, number_of_games_with_strike, game_with_a_strike

def get_final_output(final, print_out=False):
    gdb_list = []
    scores_list = []
    for batch in final:
        gdb_list.append(batch[0])
        scores_list.append(batch[1])
    new_games_database = {}
    ran_num = 0
    for i, game_batch in enumerate(gdb_list):
        game_batch = {k + ran_num: v for k, v in game_batch.items()}
        new_games_database.update(game_batch)
        ran_num = ran_num + len(game_batch)
    # pprint(new_games_database)
    best_game_id, number_strikes, a_game_with_a_strike = find_best_game(new_games_database)
    if print_out:
        # log the info to a log file
        dagster_logger.info(f"Number of games simulated: {num_games_to_simulate}")
        dagster_logger.info(f"Number of unique games: {len(new_games_database.keys())}")
        dagster_logger.info(f"Number of games with a strike: {number_strikes}")
        dagster_logger.info(f"Compare Column: {compare_column}")
        dagster_logger.info("Best game: ")
        dagster_logger.info(f"\n{new_games_database[best_game_id]}")
        dagster_logger.info(f"{datetime.datetime.now() - today}")
    return best_game_id, number_strikes, a_game_with_a_strike, ran_num, new_games_database


# RAY FUNCTIONS
def get_materialized_futures(results):
    materialized_results = []
    for x in results:
        try:
            materialized_results.append(ray.get(x))
        except:
            try:
                # if it's a list
                for y in x:
                    materialized_results.append(ray.get(y))
            except:
                # 1 level lower if nested
                z = ray.get(x)
                if type(z) == list:
                    p = ray.get(z)
                    print(x, p, z)
                    materialized_results.append(p)
                else:
                    materialized_results.append(z)
    return materialized_results

@ray.remote
class ProgressActor:
    def __init__(self, total_num_samples: int):
        self.total_num_samples = total_num_samples
        self.num_samples_completed_per_task = {}
        self.count = 0

    def report_progress(self, task_id: int, num_samples_completed: int) -> None:
        self.num_samples_completed_per_task[task_id] = num_samples_completed

    def get_progress(self) -> float:
        return (
            sum(self.num_samples_completed_per_task.values()) / self.total_num_samples
        )

    def adder(self, numm):
        self.count = self.count + numm

    def counterr(self):
        return self.count
@ray.remote
def ray_simulate_multiple_games(games_by_score: dict,
                  game_template: pd.DataFrame,
                  compare_column:str = compare_column,
                  num_games_to_simulate:int = num_games_to_simulate,
                            ) -> tuple:
    num_games_simulate = range(1, num_games_to_simulate + 1)
    games_database = {x: pd.DataFrame() for x in num_games_simulate}

    for game_no in range(1, num_games_to_simulate + 1):
        # run a simulation and find it's final score
        each_game = get_scores(find_marks(simulate_game(game_template))).copy()
        each_game_max_score = each_game['running_score'].max()
        games_database[game_no] = each_game
        g_s = games_by_score[each_game_max_score]
        g_s.append(game_no)
        # iterate through each game
        for index, sub_each in enumerate(g_s):
            try:
                # find the latest game to compare current with
                compare_game = games_database[g_s[index]]
                counter = 0
                if not each_game[compare_column].equals(compare_game[compare_column]):
                    counter = counter + 1
                # if each_game is unique, then add it to the game database and score database
                if counter == len(g_s):
                    games_database[game_no] = each_game
                    g_s.append(game_no)
            except Exception as eee:
                pass
    return games_database, games_by_score
@ray.remote
def sampling_task(num_samples: int, progress_actor: ray.actor.ActorHandle) -> tuple:
    flag = 0
    games_database_, games_by_score_ = ray.get(ray_simulate_multiple_games.remote(games_by_score=base_games_by_score,
                                                          game_template=base_game_template,
                                                          compare_column=compare_column,
                                                          num_games_to_simulate=num_samples, )
                               )
    progress_actor.adder.remote(num_samples)
    best_game_id, number_strikes, a_game_with_a_strike = find_best_game(games_database_)
    high = games_database_[best_game_id]["running_score"].max()
    return games_database_, games_by_score_, high

base_game_template = create_game_template()

# Create the progress actor.
progress_actor = ProgressActor.remote(num_games_to_simulate)

# Create and execute all sampling tasks in parallel.
other = []
flagg = 0
highscore = 0
for _ in range(loops):
    if flagg:
        break
    results = [sampling_task.remote(NUM_SAMPLES_PER_TASK, progress_actor) for i in range(NUM_SAMPLING_TASKS)]
    for gdb, gbs, hgh in ray.get(results):
        other.append((gdb, gbs, hgh))
        if hgh > highscore:
            highscore = hgh
            if highscore > high_score:
                print("OVER HIGH SCORE")
                break
    print(f'progress_outer: {ray.get(progress_actor.counterr.remote())} | high_score: {highscore} | time: {datetime.datetime.now()-today}', )

print('end: ', ray.get(progress_actor.counterr.remote()))
print('max: ', num_games_to_simulate)

get_final_output(other, print_out=True)
