// Action creators
const createSendAction = (msg) => ({
  type: 'SEND',
  payload: msg
});

const createSetAlarmAction = (id) => ({
  type: 'SET_ALARM',
  payload: { id }
});

// Special actions
const NO_ACTIONS = [];
const IGNORE_MSG = { type: 'IGNORE' };

// Helper to combine multiple actions
const combineActions = (...actions) => actions.flat();

module.exports = {
  createSendAction,
  createSetAlarmAction,
  NO_ACTIONS,
  IGNORE_MSG,
  combineActions
};
