

const lexResponses = require('./lexResponses');

function helpBookRoom(roomType){
  console.log("Reached helpBookRoom", roomType)
  if (roomType == 'meeting') {
    callback(lexResponses.elicitIntent(
            intentRequest.sessionAttributes, "You can book meeting rooms from http://amzm.com/BookingTool"));
  }
  if (roomType == 'meditation') {
    callback(lexResponses.elicitIntent(
            intentRequest.sessionAttributes, "We have meditation rooms available on ground floor of each building. Just head towards the micro kitchen"));

  }
  if (roomType == 'doctor') {
    callback(lexResponses.elicitIntent(
            intentRequest.sessionAttributes, "All floors are equipped with first aid box. Otherwise head to building number 2 or 14 for immediate help."));
  }
}

module.exports = function(intentRequest, callback) {
  var roomType = intentRequest.currentIntent.slots.room_type;
  console.log('Inputs are: ' + roomType);

   const source = intentRequest.invocationSource;

    if (source === 'DialogCodeHook') { // I think it is to represent the input coming from bot
      console.log('It is a dialog code hook. Reached here ')
      helpBookRoom(roomType)
      return;
    } else {
      console.log('source was something else :', source)
    }
    callback(lexResponses.delegate(intentRequest.sessionAttributes, intentRequest.currentIntent.slots));
    return;
}
