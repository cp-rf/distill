const Events = {
  APPEND_REQ: 'APPEND_REQ',
  APPEND_RESP: 'APPEND_RESP',
  VOTE_REQ: 'VOTE_REQ',
  VOTE_RESP: 'VOTE_RESP',
  CMD_REQ: 'CMD_REQ',
  CMD_RESP: 'CMD_RESP',
  TIMEOUT: 'TIMEOUT'
};

const responseType = {
  [Events.APPEND_REQ]: Events.APPEND_RESP,
  [Events.VOTE_REQ]: Events.VOTE_RESP,
  [Events.CMD_REQ]: Events.CMD_RESP
};

module.exports = {
  Events,
  responseType
};
