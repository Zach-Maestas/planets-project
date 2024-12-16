const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify');
const fs = require('fs');

const habitablePlanets = [];

const readableStream = fs.createReadStream('kepler_data.csv');
const parser = parse({
    comment: '#',
    columns: true,
});

readableStream
    .pipe(parser)
    .on('data', (data) => {
        if (isHabitablePlanet(data)){
            habitablePlanets.push(data);
        }
    })
    .on('end', () => {
        console.log('Finished Processing...');
        console.log(habitablePlanets);
        console.log(`${habitablePlanets.length} Planets Found...`);
        habitablePlanetsToCSV();
    });

// Handling errors on the readable stream
readableStream.on('error', (err) => {
    console.error('Error while reading file:', err);
});

// Handling errors on the parser stream
parser.on('error', (err) => {
    console.error('Error while parsing CSV:', err);
});

function isHabitablePlanet(planet){
    return planet.koi_disposition == 'CONFIRMED'
    && planet.koi_insol > 0.36 && planet.koi_insol < 1.11
    && planet.koi_prad < 1.6;
}

function habitablePlanetsToCSV() {
    if (habitablePlanets.length === 0) {
        console.log("No habitable planets to save.");
        return;
    }

    const columns = extractColumns(); 

    stringify(habitablePlanets, {
        header: true,
        columns: columns
    }, (err, output) => {
        if (err) {
            console.error('Error stringifying data:', err);
            return;
        }
        writeToCSV(output);  // Pass the CSV string output to writeToCSV
    });
}

function extractColumns(){
    return Object.keys(habitablePlanets[0]).reduce((acc, key) => {
        acc[key] = key;  // Map each key to itself
        return acc;
    }, {});
}

function writeToCSV(csvString) {
    fs.writeFile('habitablePlanets.csv', csvString, (err) => {
        if (err) {
            console.error('Error writing CSV file:', err);
            return;
        }
        console.log('Successfully saved habitable planets to CSV!');
    });
}