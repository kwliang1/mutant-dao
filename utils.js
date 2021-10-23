const fs = require("fs");
const path = require("path");
const log = require('loglevel');
const prefix = require('loglevel-plugin-prefix');

//setting up logger
prefix.reg(log);
log.enableAll();

prefix.apply(log, {
	template: '[%t] %l (%n) static text:',
	levelFormatter(level) {
		return level.toUpperCase();
	},
	nameFormatter(name) {
		return name || 'global';
	},
	timestampFormatter(date) {
		return date.toISOString();
	},
});
//end logger setup


const appLoggingTag = `[UTILS]`;

const getABIForCollection = async ({id = ''} = {}) => {
	const loggingTag = `${appLoggingTag}[getABIForCollection]`;
	let abi = '';
	try{
		if(id.length > 0){
			let rawdata = fs.readFileSync(path.resolve(__dirname, `./abi.json`));
			if(rawdata.length > 0){
				abi = JSON.parse(rawdata);
			} else {
				throw new Error(`${loggingTag} No abi found for collection w/ id: "${id}"`);
			}
		} else {
			throw new Error(`${loggingTag} Missing ID to retrieve ABI for!`);
		}
	} catch (e){
		console.error(`${loggingTag} Error:`, e);
		throw e;
	}
	return abi;
}

module.exports = {
	getABI: getABIForCollection,
	logger: log
}