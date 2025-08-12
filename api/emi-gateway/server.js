'use strict';


if (process.env.NODE_ENV !== 'production') {
    require('dotenv').load();
}

try {
    const { map, mergeMap, catchError } = require('rxjs/operators');
    const { of } = require('rxjs');
    const { ApolloServer, makeExecutableSchema, AuthenticationError } = require('apollo-server');
    const gqlSchema = require('./graphql/index');
    const broker = require('./broker/BrokerFactory')();
    const jsonwebtoken = require('jsonwebtoken');

    //graphql types compendium
    const typeDefs = gqlSchema.types;
    //graphql resolvers compendium
    const resolvers = gqlSchema.resolvers;
    //graphql schema = join types & resolvers
    const schema = makeExecutableSchema({ typeDefs, resolvers });

    //Service Port
    const PORT = process.env.GRAPHQL_END_POINT_PORT || 3000;
    const jwtFleetPublicKey = (process.env.JWT_PUBLIC_KEY || "").replace(/\\n/g, '\n');
    const jwtDevicePublicKey = (process.env.JWT_DEVICE_PUBLIC_KEY || "").replace(/\\n/g, '\n');
    let websocketsClients = 0;
    // GraphQL: Schema
    const server = new ApolloServer({
        schema,
        context: async ({ req, connection }) => {
            if (connection) {
                // check connection for metadata
                return connection.context;
            } else {
                const encodedToken = (req.headers.authorization || "").replace('Bearer ', '');
                try {
                    const decodedFleetToken = jsonwebtoken.verify(encodedToken, jwtFleetPublicKey, { algorithms: ['RS256'] });    
                    return ({
                        authToken: decodedFleetToken,
                        encodedToken: req.headers.authorization ? req.headers.authorization.replace(/Bearer /i, '') : undefined,
                        broker
                    });
                } catch (error) {
                    try {
                        const decodedDeviceToken = jsonwebtoken.verify(encodedToken, jwtDevicePublicKey, { algorithms: ['RS256'] });    
                        return ({
                            authToken: decodedDeviceToken,
                            encodedToken: req.headers.authorization ? req.headers.authorization.replace(/Bearer /i, '') : undefined,
                            broker
                        });
                    } catch(err)   {
                        console.log('Failed to decode Token',error,req.headers);
                        throw new AuthenticationError('you must be logged in');
                    }
                }
            }
        },
        introspection: true,
        //playground: true,
        playground: {
            endpoint: process.env.GRAPHIQL_HTTP_END_POINT,
            subscriptionEndpoint: `ws://${process.env.GRAPHQL_END_POINT_HOST}:${process.env.GRAPHQL_END_POINT_PORT}${process.env.GRAPHQL_WS_END_POINT}`,
            settings: {
                'editor.theme': 'dark'
            }
        },
        subscriptions: {
            path: process.env.GRAPHQL_WS_END_POINT,
            onConnect: async (connectionParams, webSocket, connectionContext) => {
                websocketsClients++;
                //console.log(new Date().toUTCString(), ': webSocket.onConnect, currentClients = ', websocketsClients);
                //console.log(`GraphQL_WS.onConnect: origin=${connectionContext.request.headers.origin} url=${connectionContext.request.url}`);
                const encondedToken$ = connectionParams.authToken
                    ? of(connectionParams.authToken)
                    : connectionContext.request.headers.authorization
                        ? of(connectionContext.request.headers.authorization)
                            .pipe(map(header => header.replace('Bearer ', '')))
                        : undefined;
                if (!encondedToken$) {
                    throw new Error('Missing auth token!');
                }
                //this is the default action to do when unsuscribing                
                const authToken = await encondedToken$.pipe(
                    mergeMap(encondedToken => {
                        return of(jsonwebtoken.verify(encondedToken, jwtFleetPublicKey, { algorithms: ['RS256'] })).pipe(
                            catchError(err => {
                                return of(jsonwebtoken.verify(encondedToken, jwtDevicePublicKey, { algorithms: ['RS256'] }))
                            })
                        )
                    })
                )
                    .toPromise()
                    .catch(error => console.error(`Failed to verify jwt token on WebSocket channel: ${error.message}`, error));
                const encondedToken = await encondedToken$.toPromise()
                    .catch(error => console.error(`Failed to extract decoded jwt token on WebSocket channel: ${error.message}`, error));
                return { broker, authToken, encondedToken, webSocket };
            },
            onDisconnect: (webSocket, connectionContext) => {
                websocketsClients--;
                if (webSocket.onUnSubscribe) {
                    webSocket.onUnSubscribe.subscribe(
                        (evt) => {
                            console.log(`webSocket.onUnSubscribe: ${JSON.stringify({ evt })};  origin=${connectionContext.request.headers.origin} url=${connectionContext.request.url};`);
                        },
                        error => console.error(`GraphQL_WS.onDisconnect + onUnSubscribe; origin=${connectionContext.request.headers.origin} url=${connectionContext.request.url}; Error: ${error.message}`, error),
                        () => {
                            console.log(`GraphQL_WS.onDisconnect + onUnSubscribe: Completed OK; origin=${connectionContext.request.headers.origin} url=${connectionContext.request.url};`);
                        }
                    );
                } else {
                    //console.log(`GraphQL_WS.onDisconnect; origin=${connectionContext.request.headers.origin} url=${connectionContext.request.url}; WARN: no onUnSubscribe callback found`);
                }
            },
        },
        engine: {
            reportSchema: true,
            key: process.env.APOLLO_ENGINE_API_KEY,
            logging: {
                level: process.env.APOLLO_ENGINE_LOG_LEVEL // opts: DEBUG, INFO (default), WARN or ERROR.
            }
        },
        plugins: [{
            serverWillStart() {
                console.log("SERVER WILL START");
                return {
                    serverWillStop() {
                        console.log("SERVER WILL STOP");
                    }
                };
            },
            requestDidStart({ context }) {
                return {
                    async didEncounterErrors({ errors }) {
                        console.log("ERROR =====>", errors);
                    }
                };
            }
        }],
        tracing: true,
        cacheControl: true
    });

    server.listen({
        port: PORT,
    }).then(({ url }) => {
        console.log(`Apollo Server is now running on http://localhost:${PORT}`);
        console.log(`HTTP END POINT: http://${process.env.GRAPHQL_END_POINT_HOST}:${process.env.GRAPHQL_END_POINT_PORT}${process.env.GRAPHQL_HTTP_END_POINT}`);
        console.log(`WEBSOCKET END POINT: ws://${process.env.GRAPHQL_END_POINT_HOST}:${process.env.GRAPHQL_END_POINT_PORT}${process.env.GRAPHQL_WS_END_POINT}`);
        console.log(`GRAPHIQL PAGE: http://${process.env.GRAPHQL_END_POINT_HOST}:${process.env.GRAPHQL_END_POINT_PORT}${process.env.GRAPHIQL_HTTP_END_POINT}`);
    }).catch(err => {
        console.log('There was an error starting the server or Engine.');
        console.error(err);
        process.exit(1);
    });




    process.on('exit', (code) => {
        console.log(`====>Server.onExit: About to exit with code: ${code}`);
    });
    process.on('uncaughtException', (err, origin) => {
        console.log(`====>Server.onuncaughtException: ${process.stderr.fd}, Caught exception: ${err}, Exception origin: ${origin}`);
    });
    process.on('warning', (warning) => {
        console.warn('====>Server.onwarning', warning.name);    // Print the warning name
        console.warn('====>Server.onwarning', warning.message); // Print the warning message
        console.warn('====>Server.onwarning', warning.stack);   // Print the stack trace
    });
    process.on('SIGINT', () => {
        console.log('====>Server.onSIGINT: Received SIGINT.');
    });
    process.on('SIGTERM', () => {
        console.log('====>Server.onSIGTERM: Received SIGTERM.');
    });

} catch (error) {
    console.error('Server error', error);
    console.log(error.stack);
}