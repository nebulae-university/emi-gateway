const path = require('path');
const mergeGraphqlSchemas = require('merge-graphql-schemas');
const fileLoader = mergeGraphqlSchemas.fileLoader;
const mergeTypes = mergeGraphqlSchemas.mergeTypes;
const mergeResolvers = mergeGraphqlSchemas.mergeResolvers;

const typesArray = fileLoader(
    path.join(__dirname, '.'),
    { extensions: ['.graphql', '.graphqls','.gql'], recursive: true }
);
const resolversArray = fileLoader(
    path.join(__dirname, '.'),
    { extensions: ['.js'], recursive: true }
);



module.exports = {
    types: mergeTypes(typesArray, { all: true }),
    resolvers: mergeResolvers(resolversArray)
}