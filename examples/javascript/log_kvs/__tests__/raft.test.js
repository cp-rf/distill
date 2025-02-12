const { Events } = require('../events.js');
const { createSendAction, NO_ACTIONS } = require('../actions.js');
const { start, Status, processMsg, becomeFollower } = require('../raft.js');

// Helper functions
const createLeader = (clusterSize) => {
  const members = Array.from({ length: clusterSize }, (_, i) => `S${i + 1}`);
  const { newState } = start(members[0], members.slice(1), true);
  
  // Create sample log
  const terms = [1, 1, 1];
  newState.log = mkSampleLog(terms);
  newState.pendingResponses = mkPendingResponses(terms.length);
  
  return newState;
};

const createFollower = (clusterSize) => {
  const state = createLeader(clusterSize);
  const { newState } = becomeFollower(state);
  return {
    ...newState,
    log: mkSampleLog([1, 1, 1]) // Match Java implementation's default log
  };
};

const mkSampleLog = (terms) => {
  return terms.map((term, i) => ({
    term,
    cl_reqid: i.toString(),
    cmd: 'I',
    key: 'a',
    by: 10
  }));
};

const mkPendingResponses = (length) => {
  const pending = new Map();
  for (let i = 0; i < length; i++) {
    const reqid = i.toString();
    const msg = {
      from: 'cl1',
      to: 'S1',
      type: Events.CMD_REQ,
      reqid,
      cmd: 'W',
      key: 'a',
      value: i
    };
    pending.set(reqid, msg);
  }
  return pending;
};

const extractSends = (actions) => {
  return actions.filter(action => action.type === 'SEND')
    .map(action => action.payload);
};

const extractSend = (actions, to) => {
  return extractSends(actions)
    .find(msg => msg.to === to) || null;
};

const assertJsonEquals = (a, b) => {
  // Handle null cases
  if (a === null || b === null) {
    expect(a).toBe(b);
    return;
  }

  // Handle arrays
  if (Array.isArray(a) && Array.isArray(b)) {
    expect(a.length).toBe(b.length);
    a.forEach((item, i) => assertJsonEquals(item, b[i]));
    return;
  }

  // Handle objects
  if (typeof a === 'object' && typeof b === 'object') {
    const aKeys = Object.keys(a);
    const bKeys = Object.keys(b);
    expect(aKeys.length).toBe(bKeys.length);
    aKeys.forEach(key => {
      expect(b).toHaveProperty(key);
      assertJsonEquals(a[key], b[key]);
    });
    return;
  }

  // Handle primitives
  expect(a).toBe(b);
};

// Test cases
describe('Raft', () => {
  test('downgrade leader on higher term', () => {
    const leader = createLeader(3);
    const msg = {
      from: 'S3',
      to: 'S1',
      type: Events.APPEND_REQ,
      term: 5,
      entries: [],
      index: 0,
      num_committed: 0
    };

    const { newState } = processMsg(leader, msg);
    expect(newState.status).toBe(Status.FOLLOWER);
    expect(newState.term).toBe(5);
  });

  test('log replication', () => {
    // Create leader with specific log
    const leader = createLeader(3);
    leader.term = 3;
    leader.log = mkSampleLog([1, 2, 2]);
    leader.followers.get('S2').logLength = 0;
    leader.followers.get('S3').requestPending = true;

    // Create follower with empty log
    const follower = createFollower(3);
    follower.log = [];
    follower.myId = 'S2';

    // Run replication cycle
    let msg = {
      type: Events.CMD_REQ,
      from: 'client',
      to: 'S1',
      cmd: 'R',
      key: 'a',
      reqid: '1.client'
    };

    let currentLeaderState = { ...leader };
    let currentFollowerState = { ...follower };

    for (let i = 0; i < 6; i++) {
      const { newState: newLeaderState, actions: leaderActions } = processMsg(currentLeaderState, msg);
      currentLeaderState = newLeaderState;
      
      msg = extractSend(leaderActions, 'S2');
      if (!msg) break;

      const { newState: newFollowerState, actions: followerActions } = processMsg(currentFollowerState, msg);
      currentFollowerState = newFollowerState;
      
      msg = extractSend(followerActions, 'S1');
    }

    // Update final states for assertions
    leader = currentLeaderState;
    follower = currentFollowerState;

    expect(msg).toBeNull();
    assertJsonEquals(leader.log, follower.log);
    expect(leader.numCommitted).toBe(3);
    expect(leader.numApplied).toBe(3);
  });
});
