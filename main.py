"""

    """

import pandas as pd
from githubdata import GithubData
from mirutil.df_utils import save_as_prq_wo_index as sprq


class RepoUrl :
    src = 'https://github.com/imahdimir/raw-d-codal_ir-nasheran'
    cur = 'https://github.com/imahdimir/u-d-firm-ISIC-in-codal'
    trg = 'https://github.com/imahdimir/d-firm-ISIC-in-codal'

ru = RepoUrl()

class ColName :
    ctic = 'CodalTicker'
    obsd = 'ObsDate'
    isic = 'ISIC'

cn = ColName()

def main() :
    pass

    ##

    gd_src = GithubData(ru.src)
    dfs = gd_src.read_data()
    ##
    msk = dfs['0'].eq('ISIC :')
    dfs = dfs[msk]
    ##
    assert dfs['1'].eq(dfs['6']).all()
    ##
    dfs = dfs[['1' , cn.ctic , cn.obsd]]
    ##
    dfs.rename(
        columns = {
                '1' : cn.isic
                } ,
        inplace = True
        )
    ##
    dfs.dropna(inplace = True)
    ##
    msk = dfs.duplicated(subset = [cn.ctic , cn.obsd] , keep = False)
    df1 = dfs[msk]
    assert not msk.any()
    ##

    gd_trg = GithubData(ru.trg)
    gd_trg.overwriting_clone()
    ##
    dft = gd_trg.read_data()
    ##
    dft = pd.DataFrame()
    ##
    dft = pd.concat([dft , dfs] , axis = 0 , ignore_index = True)
    ##
    dft.drop_duplicates(inplace = True)
    ##
    sprq(dft , gd_trg.local_path / 'data.prq')
    ##
    msg = 'data updated by: '
    msg += ru.cur
    ##
    gd_trg.commit_and_push(msg)
    ##

    gd_src.rmdir()
    gd_trg.rmdir()


    ##

    ##

##
if __name__ == "__main__" :
    main()

##
# noinspection PyUnreachableCode
if False :

    pass

    ##


    ##

    ##

##

##
