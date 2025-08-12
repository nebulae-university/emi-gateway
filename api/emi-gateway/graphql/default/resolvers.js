const bigInt = require('graphql-bigint');
const upload = require('graphql-upload');
const GraphQLJSON = require('graphql-type-json');

module.exports = {
    BigInt: bigInt,
    Upload: upload,
    JSON: GraphQLJSON
}

