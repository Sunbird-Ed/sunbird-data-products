"""
Persona wise segragation of User Self-declaration detail report
"""
import os
import shutil
from pathlib import Path

import pandas as pd
from dataproducts.util.utils import get_data_from_blob, post_data_to_blob


class UserDeclarationReport:
    def __init__(self, data_store_location, states, is_private):
        self.data_store_location = Path(data_store_location)
        self.states = states
        self.is_private = is_private

    def init(self):
        result_loc = self.data_store_location.joinpath('location')
        for slug in self.states.split(","):
            slug = slug.strip()
            state_result_loc = result_loc.joinpath(slug)
            os.makedirs(state_result_loc, exist_ok=True)
            try:
                get_data_from_blob(state_result_loc.joinpath('declared_user_detail', '{}.csv'.format(slug)),
                                   is_private=self.is_private)
                user_df = pd.read_csv(state_result_loc.joinpath('declared_user_detail', '{}.csv'.format(slug)),
                                      encoding="utf-8-sig")
                os.makedirs(state_result_loc.joinpath('personas'), exist_ok=True)
                for persona, user_data in user_df.groupby('Persona'):
                    user_data.to_csv(state_result_loc.joinpath('personas', persona.lower() + ".csv"), index=False)

                shutil.make_archive(str(state_result_loc.joinpath('declared_user_detail', slug)),
                                    'zip',
                                    str(state_result_loc.joinpath('personas')))
                post_data_to_blob(state_result_loc.joinpath('declared_user_detail', '{}.zip'.format(slug)),
                                  is_private=self.is_private)
            except pd.errors.ParserError:
                print("Unable to parse data error for " + slug)
                continue
            except Exception as e:
                print("declared_user_detail not available for " + slug)
                continue
