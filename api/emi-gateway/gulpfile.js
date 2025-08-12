const gulp = require('gulp');
const paths = {
    ignoreFromDistFolder: [
        '!graphql/default/**/*',
        '!graphql/sample/**/*',
        '!graphql/index.js',
        '!graphql/default/',
        '!graphql/sample/'],
    srcFolder: `../../api/emi-gateway/`
}
let LAST_SYNC_TIMESTAMP = 0;
const DEBOUNCE_TIME = 2000;
// gulp.task('default', ['syncSource'] );

// configure which files to watch and what tasks to use on file changes
gulp.task('dev-sync-source', () => {
    gulp.watch([
        `graphql/**/*`,
        ...paths.ignoreFromDistFolder
    ], 
    ['updateRepoFiles']);

    gulp.watch(`${paths.srcFolder}/**/*`, ['updateDevelpmentFiles'])
});

gulp.task('updateRepoFiles', ()=> {
    if( (Date.now() - LAST_SYNC_TIMESTAMP) >= DEBOUNCE_TIME){
    console.log('#################### UPDATING SRC FOLDER ####################');
    LAST_SYNC_TIMESTAMP = Date.now();
    return gulp.src([
        `graphql/**/*`,
        ...paths.ignoreFromDistFolder
    ])
    .pipe(gulp.dest(`${paths.srcFolder}/graphql`));
    }
});
gulp.task('updateDevelpmentFiles', ()=> {
    if( (Date.now() - LAST_SYNC_TIMESTAMP) >= DEBOUNCE_TIME){
        console.log('################ UPDATING DEVELOPMENT FOLDER ################');
        LAST_SYNC_TIMESTAMP = Date.now();
        return gulp.src(`${paths.srcFolder}/**/*`)
        .pipe(gulp.dest(`./`));
    }    
});