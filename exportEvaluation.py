import synapseclient as sc
import pandas as pd
import argparse

def read_args():
    parser = argparse.ArgumentParser(description='Export challenge leaderboard '
                'as pandas DataFrame.')
    parser.add_argument('evaluationId', type=int, help='ID of the evaluation queue.')
    parser.add_argument('--outputPath', help='(optional). Path to write .csv to.')
    return parser.parse_args()

def getFailureReasons(s):
    """ Get failure reason from submissions.

    Parameters
    ----------
    s : list
        A list of tuples containing a :py:class:`synapseclient.evaluation.Submission`
        and a :py:class:`synapseclient.evaluation.SubmissionStatus`.
    """
    failureReasons = []
    for t in s:
        t = t[1]
        reason = None
        for k in t['annotations']['stringAnnos']:
            if k['key'] == 'failureReason':
                reason = k['value']
        failureReasons.append(reason)
    return failureReasons

def getTeamNames(s):
    """ Get team names from submissions.

    Parameters
    ----------
    s : list
        A list of tuples containing a :py:class:`synapseclient.evaluation.Submission`
        and a :py:class:`synapseclient.evaluation.SubmissionStatus`.
    """
    teamNames = []
    for t in s:
        t = t[1]
        team = None
        for k in t['annotations']['stringAnnos']:
            if k['key'] == 'team':
                team = k['value']
        teamNames.append(team)
    return teamNames

def buildLeaderboard(s):
    """ Return leaderboard as pandas DataFrame.

    Parameters
    ----------
    s : list
        A list of tuples containing a :py:class:`synapseclient.evaluation.Submission`
        and a :py:class:`synapseclient.evaluation.SubmissionStatus`.
    """
    submissionIds = [t[1]['id'] for t in s]
    failureReasons = getFailureReasons(s)
    teamNames = getTeamNames(s)
    names = [t[0]['name'] for t in s]
    statuses = [t[1]['status'] for t in s]
    userIds = [t[0]['userId'] for t in s]
    createdOns = [t[0]['createdOn'] for t in s]
    teamIds = [t[0]['teamId'] if 'teamId' in t[0] else None for t in s]
    evaluationIds = [t[0]['evaluationId'] for t in s]
    entityIds = [t[0]['entityId'] for t in s]
    """
    auprs = [t[1]['annotations']['doubleAnnos'][0]['value']
            if 'doubleAnnos' in t[1]['annotations'] else None for t in s]
    """
    leaderboard = pd.DataFrame({'createdOn': createdOns,
        'entityId': entityIds, 'evaluationId': evaluationIds, 'name': names,
        'status': statuses, 'submissionId': submissionIds, 'team': teamNames,
        'teamId': teamIds, 'userId': userIds})
    return leaderboard

if __name__ == '__main__':
    args = read_args()
    syn = sc.login()
    s = list(syn.getSubmissionBundles(args.evaluationId))
    leaderboard = buildLeaderboard(s)
    if args.outputPath:
        leaderboard.to_csv(args.outputPath, index=False)
