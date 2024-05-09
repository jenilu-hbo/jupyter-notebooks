import math
import numpy as np
import pandas as pd

META_COLUMNS = ['match_id',
                'title',
                'title_id',
                'available_date',
                'originals_type',
                'content_category',
                'real_date',
                'prediction_start_day',
                ]
TRACKING_COLUMN = {'pct_actives': 'pct_actives',
                 'total_actives': 'total_viewing_accounts',
                 'title_actives': 'title_viewing_accounts',
                 }

class DecayModel:
    def __init__(self, kpi):
        self.tracking_col = TRACKING_COLUMN[kpi]
        self.multiplier_df = None

    def fit(self, train_df):
        pct_actives = train_df
        pct_actives_from = pct_actives[['originals_type', 'content_category',
                                    'match_id','prediction_start_day', self.tracking_col]]
        pct_actives_from.rename(columns={self.tracking_col: self.tracking_col + '_from'}, inplace=True)


        pct_actives_to = pct_actives[['originals_type', 'content_category',
                                        'match_id','prediction_start_day', self.tracking_col]]
        pct_actives_to.rename(columns={self.tracking_col: self.tracking_col + '_to',
                                         'prediction_start_day': 'days_after_launch'}, inplace=True)


        multipliers = pd.merge(pct_actives_from, pct_actives_to,
                           on=['originals_type', 'content_category',
                               'match_id'])
        multipliers['multiplier'] = multipliers[self.tracking_col + '_to'] / multipliers[self.tracking_col+ '_from']
        multiplier_df = multipliers.groupby(['originals_type', 'content_category',
                                             'prediction_start_day', 'days_after_launch'],
                                            as_index=False).agg({'multiplier': 'median'})
        self.multiplier_df = multiplier_df

    def predict(self, pred_df):

        postlaunch_df = pred_df[META_COLUMNS + [self.tracking_col]]

        assert self.tracking_col in postlaunch_df.columns


        postlaunch_df = pd.merge(postlaunch_df, self.multiplier_df,
                                 on=['originals_type', 'content_category', 'prediction_start_day'],
                                 how='left')

        postlaunch_prediction = np.where(postlaunch_df['prediction_start_day'] > postlaunch_df['days_after_launch'],
                                         np.nan,
                                         postlaunch_df[self.tracking_col] * postlaunch_df['multiplier'])

        postlaunch_df['prediction'] = postlaunch_prediction


        postlaunch_df = postlaunch_df[META_COLUMNS + ['days_after_launch', 'prediction']]

        return postlaunch_df