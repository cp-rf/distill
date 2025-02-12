const { Events, responseType } = require('./events.js');
const { createSendAction, createSetAlarmAction, NO_ACTIONS, IGNORE_MSG, combineActions } = require('./actions.js');

// Constants
const Status = {
  LEADER: 'LEADER',
  FOLLOWER: 'FOLLOWER'
};

// State factories
const createFollowerInfo = (id, logLength) => ({
  follower_id: id,
  logLength,
  requestPending: false
});

const createRaftState = (myId, siblings, isLeader) => ({
  myId,
  siblings,
  quorumSize: Math.floor(siblings.length + 1) / 2 + 1,
  term: isLeader ? 1 : 0,
  status: isLeader ? Status.LEADER : Status.FOLLOWER,
  kv: new Map(),
  log: [],
  numCommitted: 0,
  numApplied: 0,
  followers: null,
  pendingResponses: null
});

// Helper functions
const mkMsg = (pairs) => {
  if (pairs.length % 2 !== 0) {
    throw new Error("pairs must be even numbered");
  }
  return pairs.reduce((obj, val, idx) => {
    if (idx % 2 === 0) {
      obj[val] = pairs[idx + 1];
    }
    return obj;
  }, {});
};

const mkReply = (state, msg, ...extraPairs) => {
  const reply = mkMsg(
    'from', state.myId,
    'to', msg.from,
    'term', state.term
  );

  if (msg.reqid) {
    reply.reqid = msg.reqid;
  }

  const reqType = msg.type;
  const respType = responseType[reqType];
  if (!respType) {
    throw new Error("msg type error: " + JSON.stringify(msg));
  }
  reply.type = respType;

  for (let i = 0; i < extraPairs.length; i += 2) {
    reply[extraPairs[i]] = extraPairs[i + 1];
  }

  return createSendAction(reply);
};

// Core functions
const becomeLeader = (state) => {
  const newState = {
    ...state,
    status: Status.LEADER,
    followers: new Map(),
    pendingResponses: new Map()
  };

  state.siblings.forEach(fol => {
    newState.followers.set(fol, createFollowerInfo(fol, state.log.length));
  });

  return { newState, actions: NO_ACTIONS }; // There will be actions in a later exercise.
};

const becomeFollower = (state) => {
  return {
    newState: {
      ...state,
      status: Status.FOLLOWER,
      followers: null
    },
    actions: NO_ACTIONS // There will be actions in a later exercise.
  };
};

const onAppendReq = (state, msg) => {
  if (state.status === Status.LEADER) {
    throw new Error("Leader received AppendReq");
  }

  const msgIndex = msg.index;
  const msgEntries = msg.entries;
  let toSend = null;

  if (msgIndex > state.log.length) {
    // Ask leader to back up
    // TODO: Return Send action "success": "false" and "index" set to current log length
    throw new Error("UNIMPLEMENTED");
  } else {
    if (msgIndex === state.log.length) {
      // TODO: Append msgEntries to log
      // TODO: Return Send action "success": "true" and "index" set to current log length
      throw new Error("UNIMPLEMENTED");
    } else { // msgIndex < log.length
      // TODO: chop tail until msgIndex, then add msgEntries
      throw new Error("UNIMPLEMENTED");
    }
  }

  const actions = [toSend];

  if (state.status !== Status.LEADER) {
    const newState = {
      ...state,
      numCommitted: msg.num_committed
    };
    const { newActions } = onCommit(newState);
    actions.push(...newActions);
    return { newState, actions };
  }

  return { newState: state, actions };
};

const onAppendResp = (state, msg) => {
  if (state.status !== Status.LEADER) {
    throw new Error("Non-leader received AppendResp");
  }

  const msgIndex = msg.index;
  if (msgIndex > state.log.length) {
    throw new Error(`Invalid msgIndex: ${msgIndex}`);
  }

  const follower = state.followers.get(msg.from);
  const newFollower = {
    ...follower,
    logLength: msgIndex,
    requestPending: false
  };

  const newState = {
    ...state,
    followers: new Map(state.followers).set(msg.from, newFollower)
  };

  const actions = [createSetAlarmAction(follower.follower_id)]; // heartbeat timer reset

  if (updateNumCommitted(newState)) {
    const { actions: commitActions } = onCommit(newState);
    actions.push(...commitActions);
  }

  return { newState, actions };
};

const updateNumCommitted = (state) => {
  if (state.status !== Status.LEADER) {
    throw new Error("Non-leader called updateNumCommitted");
  }

  // TODO: IMPLEMENT ABOVE.
  // return true if numCommitted was changed.
  throw new Error("UNIMPLEMENTED");
};

const apply = (state, index, entry) => {
  const key = entry.key;
  const cmd = entry.cmd;
  let action = NO_ACTIONS;
  let clientMsg = null;

  if (state.status === Status.LEADER) {
    const reqid = entry.cl_reqid;
    clientMsg = state.pendingResponses.get(reqid);
    state.pendingResponses.delete(reqid);
  }

  if (cmd === 'W') {
    const value = entry.value;
    state.kv.set(key, value);
    if (clientMsg) {
      action = mkReply(state, clientMsg, 'client_msg', clientMsg, 'index', index);
    }
  }

  return { newState: state, action };
};

const onCommit = (state) => {
  // TODO: For each index starting from numApplied to numCommitted
  // TODO:      call apply with that log entry
  const newState = {
    ...state,
    numApplied: state.numCommitted
  };
  throw new Error("UNIMPLEMENTED");
};

const mkAppendMsg = (state, to, index) => {
  if (state.status !== Status.LEADER) {
    throw new Error("Non-leader called mkAppendMsg");
  }

  // TODO: Create an APPEND_REQ message with
  // TODO: attributes "index", "num_committed", "term"
  // TODO: and "entries". This last attribute should be a slice of the
  // TODO: log from index to end of log.
  throw new Error("UNIMPLEMENTED");
};

const sendAppends = (state) => {
  if (state.status !== Status.LEADER) {
    return { newState: state, actions: NO_ACTIONS };
  }

  // TODO: For each follower,
  // TODO:   if fi.logLength < log.length and not fi.requestPending
  // TODO:      create a send action with an appendReq message (use mkAppendMsg)
  // TODO:      set fi.requestPending
  throw new Error("UNIMPLEMENTED");
};

const checkTerm = (state, msg) => {
  const msgTerm = msg.term;

  // TODO: if the incoming message's term is > my term
  // TODO:     upgrade my term
  // TODO:     if I am a leader, becomeFollower()
  throw new Error("UNIMPLEMENTED");
};

const onClientCommand = (state, msg) => {
  if (state.status !== Status.LEADER) {
    return {
      newState: state,
      action: mkReply(state, msg, 'errmsg', 'Not a leader')
    };
  }

  let action = NO_ACTIONS;
  let newState = state;

  switch (msg.cmd) {
    case 'R': {
      const key = msg.key;
      const value = state.kv.get(key);
      action = mkReply(state, msg, 'value', value);
      break;
    }
    case 'W': {
      newState = {
        ...state,
        pendingResponses: new Map(state.pendingResponses).set(msg.reqid, msg)
      };
      newState = replicate(newState, msg);
      break;
    }
    default:
      throw new Error("Unknown cmd " + msg.cmd);
  }

  return { newState, action };
};

const replicate = (state, msg) => {
  const entry = mkMsg(
    'term', state.term,
    'cl_reqid', msg.reqid,
    'key', msg.key,
    'cmd', msg.cmd,
    'value', msg.value
  );

  return {
    ...state,
    log: [...state.log, entry]
  };
};

const processMsg = (state, msg) => {
  const msgType = msg.type;
  let actions = [];
  let newState = state;

  if (msgType !== Events.CMD_REQ) {
    const { newState: termState, action } = checkTerm(state, msg);
    newState = termState;
    if (action === IGNORE_MSG) {
      return { newState, actions: NO_ACTIONS };
    }
  }

  let result;
  switch (msgType) {
    case Events.APPEND_REQ:
      result = onAppendReq(newState, msg);
      break;
    case Events.APPEND_RESP:
      result = onAppendResp(newState, msg);
      break;
    case Events.CMD_REQ:
      result = onClientCommand(newState, msg);
      break;
    default:
      throw new Error("Unknown msg type " + msgType);
  }

  newState = result.newState;
  actions = [...actions, ...result.actions];

  if (newState.status === Status.LEADER) {
    const { newState: finalState, actions: appendActions } = sendAppends(newState);
    newState = finalState;
    actions = [...actions, ...appendActions];
  }

  return { newState, actions };
};

// Initialization
const start = (myId, siblings, isLeader) => {
  const state = createRaftState(myId, siblings, isLeader);
  if (isLeader) {
    return becomeLeader(state);
  }
  return becomeFollower(state);
};

module.exports = {
  start,
  processMsg,
  Status,
  becomeFollower
};
