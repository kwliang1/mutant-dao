const Web3 = require("web3");
const web3 = new Web3('https://eth-mainnet.alchemyapi.io/v2/fH3RqImWKcY75pC03EOuzKF44cbZdJca');
const db = require("../db");
const utils = require("../utils");
const {logger: log} = require("../utils");
const fs = require("fs");
const legacyContractRewardsPausedBlockNum = 13418720,
	newContractRewardsUnpausedBlockNum = 13432911;
const appLoggingTag = `[FISH-REWARDS]`;

const getLatestEvent = async ({num = 1} = {}) => {
	const loggingTag = `${appLoggingTag}[getLatestSales]`;
	let sale = [];
	try{
		const dbClient = await db.init({name: db.name});
		try {
			const collection = await db.collection.init({client: dbClient, collection: {name: db.collection.names.mutantdao}});
			const cursor = await collection.find().sort({blockNumber: -1}).limit(num);
			while(await cursor.hasNext()){
				sale = await cursor.next()
				// log.info(`${loggingTag}[count:${await cursor.count()}] event:`, sale);
			}
		} catch(e){
			log.error(`${loggingTag} Error:`, e);
		} finally {
			db.close({client: dbClient});
		}
	} catch(e){
		log.error(`${loggingTag} Error init-ing db client:`, e);
		throw e;
	}
	return sale;
}

const getAllEvents = async ({id='mutantcats', address:contractAddress='0xaAdBA140Ae5e4c8a9eF0Cc86EA3124b446e3E46A', full_rebuild:doFullRebuild = false} = {}) => {
	const loggingTag = `[getAllEvents]`;
	
	const dbClient = await db.init({name: db.name});
	
	try{
		const contractABI = await utils.getABI({id});
		// console.info(`${loggingTag} contract abi:`, contractABI);
		// return false;
		
		const collection = await db.collection.init({
			client: dbClient,
			collection: {name: db.collection.names.mutantdao}
		});
		
		let contract = new web3.eth.Contract(contractABI, contractAddress);
		const batchSize = 1000,
			// startingBlock = 13395747;//contract creation block
			startingBlock = await web3.eth.getBlockNumber(),//contract creation block
			latestEventInDB = await getLatestEvent(),
			fromBlock = latestEventInDB.blockNumber++;//getting the block number of the latest transaction + 1 so we don't repeat events
		
		const timerLabel = `${loggingTag} time to get ALL events`;
		console.time(timerLabel);
		
		//step 1 catching up events in DB
		const events = await getAllEventsInBatches({
			from: fromBlock,
			to: startingBlock,
			contract,
			batchSize,
			dbClient,
			contractAddress,
			collection,
			full_rebuild:doFullRebuild
		});
		console.timeEnd(timerLabel);
		console.info(`${loggingTag} returned event num:`, events.length);
		
		await insertMultipleRowsIntoMutantDAOCollection({events});
		
		//rebuilding new legacy contract wallets
		await rebuildWalletProfiles({staking_wallet_address:"0xb2F43262FC23d253538ca5F7b4890f89F0EE95D9"});
		
		//rebuilding new contract wallet profiles
		await rebuildWalletProfiles({is_legacy: false, staking_wallet_address: "0xd09656a2EE7E5Ee3404fAce234e683D3337dA014"});//recursively call itself
		
		//update the rewards for wallet profiles staked in new contract
		// await compareLegacyContractStakersToCurrentStakers();
		
	} catch(e){
		console.error(`${loggingTag} error:`, e);
	} finally{
		db.close({client: dbClient});
	}
}

const getAllEventsInBatches = async ({from = 0, to = 0, contract, contractAddress, batchSize = 0, events = [], collection, full_rebuild:doFullRebuild} = {}) => {
	const loggingTag = `[getAllEventsInBatches]`;
	
	try{
		const timerLabel = `${loggingTag} time to retrieve events from ${to-from} blocks`;
		console.time(timerLabel);
		console.info(`${loggingTag}[current # events: ${events.length}] getting all events for blocks #${from} to ${to}`);
		
		const currentEvents = await contract.getPastEvents('allEvents', {
			fromBlock: from,
			toBlock: to
		});
		console.timeEnd(timerLabel);
		console.info(`${loggingTag} num events returned:`, currentEvents.length);
		
		if(doFullRebuild){
			console.info(`${loggingTag} events`, currentEvents);
			if(currentEvents.length > 0){

				events = events.concat(currentEvents);

				await getAllEventsInBatches({
					from: (from - batchSize),//going back N blocks at a time
					to: from,
					contract,
					contractAddress,
					batchSize,
					events,
					collection
				});
			} else {
				// insertMultipleRowsIntoMutantDAOCollection({events});
			}
		} else {
			return currentEvents;
			// insertMultipleRowsIntoMutantDAOCollection({events: currentEvents});
		}
		
	}	catch(e){
		console.error(`${loggingTag} Error`, e);
	} finally {
		console.info(`${loggingTag} inside of FINALLY`);
	}
	
}

const insertMultipleRowsIntoMutantDAOCollection = async ({events = []} = {}) => {
	const loggingTag = `${appLoggingTag}[insertMultipleRowsIntoMutantDAOCollection]`;
	try{
		const dbClient = await db.init({name: db.name});
		
		try{
			const collection = await db.collection.init({
				client: dbClient,
				collection: {name: db.collection.names.mutantdao}
			});
			await db.collection.insertRows({rows:events,collection})
			console.info(`${loggingTag} num events ${events.length}`);
		} catch(e){
			console.error(`${loggingTag} error inserting events!`, e);
		} finally{
			db.close({client: dbClient});
		}
	} catch(e){
		throw e;
	}
}

const rebuildWalletProfiles = async ({address = '0xaAdBA140Ae5e4c8a9eF0Cc86EA3124b446e3E46A', is_legacy:isLegacy = true, staking_wallet_address:stakingWalletAddr = ""} = {}) => {
	const loggingTag = `${appLoggingTag}[buildWalletProfiles]`;
	try {
		let events = [];
		
		const dbClient = await db.init({name: db.name});
		try{
			const collectionName = db.collection.names.mutantdao,
				transfers = await db.collection.init({client: dbClient, collection: {name: db.collection.names.mutantdao}});
			console.info(`${loggingTag} "${collectionName}" collection:`, transfers);
			
			let cursor;
			//legacy v2
			if(isLegacy){
				cursor = await transfers.find({blockNumber:{$lte:legacyContractRewardsPausedBlockNum}, event:"Transfer"}).sort({blockNumber: 1}).allowDiskUse();
			} else {
				//staking to the new contract
				cursor = await transfers.find({event:"Transfer", "returnValues.to":stakingWalletAddr}).sort({blockNumber: 1}).allowDiskUse();
				// console.info(`${loggingTag} cursor:`, cursor);
			}
			
			console.info(`${loggingTag}[count:${await cursor.count()}] event`);
			let count = 0;
			while(await cursor.hasNext()) {
				const event = await cursor.next();
				count++;
				// process doc here
				// console.info(`${loggingTag} transfer`, transfer);
				// console.info(`${loggingTag}[count:${count}]`);
				events.push(event);
			}
		} catch(e){
			console.info(`${loggingTag} Transfers Error:`, e);
		} finally {
			db.close({client: dbClient});
		}
		console.info(`${loggingTag} num_events: ${events.length}`);
		
		if(isLegacy){
			await rebuildWalletsCollection({events, address, collection_name: db.collection.names.legacy_contract_wallets, staking_wallet_address:stakingWalletAddr});
		} else {
			await rebuildWalletsCollection({events, address, collection_name: db.collection.names.new_contract_wallets, staking_wallet_address:stakingWalletAddr});
		}
		
	} catch(e){
		console.error(`${loggingTag} Error:`, e);
	}
}

const calculateFishRewardsForWallets = async ({} = {}) => {
	const loggingTag = `${appLoggingTag}[compareLegacyContractStakersToCurrentStakers]`;
	try{
		const dbClient = await db.init({name: db.name});
		try{
			
			let newContractWalletCollection = await db.collection.init({client: dbClient, collection: {name: db.collection.names.new_contract_wallets}});
			const newWalletCursor = await newContractWalletCollection.find(),
				newStakingContractWalletAddressMap = {},
				arrayOfNewStakingContractWalletAddresses = [];
			
			while(await newWalletCursor.hasNext()) {
				const wallet = await newWalletCursor.next();
				if(!(wallet.address in newStakingContractWalletAddressMap)){//address not already in array
					let tokenStakingMap = wallet.token_staking_status_map,
						tokensOwned = Object.keys(tokenStakingMap);
					
					for(let i = 0; i < tokensOwned.length; i++){
						const latestNewStakingTransferEvent = "latest_stake_event" in tokenStakingMap[tokensOwned[i]] ? tokenStakingMap[tokensOwned[i]].latest_stake_event : {},
							latestNewUnStakingTransferEvent = "latest_unstake_event" in tokenStakingMap[tokensOwned[i]] ? tokenStakingMap[tokensOwned[i]].latest_unstake_event : {};
						if(
							(typeof latestNewStakingTransferEvent.blockNumber === "number") &&//user has staked to the new contract
							(
								(typeof latestNewUnStakingTransferEvent.blockNumber === "undefined") ||//user has not unstaked from the new contract	
								(latestNewUnStakingTransferEvent.blockNumber < latestNewStakingTransferEvent.blockNumber)
							)
						){
							arrayOfNewStakingContractWalletAddresses.push(wallet.address);
							newStakingContractWalletAddressMap[wallet.address] = wallet;
						}
					}
					// arrayOfNewStakingContractWalletAddresses[wallet.address];
				}
			}
			
			let legacyContractWalletCollection = await db.collection.init({client: dbClient, collection: {name: db.collection.names.legacy_contract_wallets}});
			const legacyWalletCursor = await legacyContractWalletCollection.find().sort({num_tokens: 1});
			
			while(await legacyWalletCursor.hasNext()) {
				const wallet = await legacyWalletCursor.next();
				
				// if(arrayOfNewStakingContractWalletAddresses.indexOf(wallet.address) > -1){
					console.info(`${loggingTag} wallet address: "${wallet.address}" previously staked! calculating rewards based on currently staked cats...`);
					
					const calculateRewards = async ({legacy_wallet:legacyWallet, new_wallet:newContractStakingWallet} = {}) => {
						const loggingTag = `${appLoggingTag}[calculateRewards]`;
						let rewards = 0;
						try{
							const tokensOwned = "tokens_owned" in legacyWallet ? legacyWallet.tokens_owned : [];
							
							const numBlockPerDay = 6000,
								rewardsRatePerFish = ({newContractStaking} = {}) => {
									const tsRewardEnd = (typeof newContractStaking === "object" && typeof newContractStaking.blockNumber === "number" && newContractStaking.blockNumber > newContractRewardsUnpausedBlockNum) ? newContractStaking.blockNumber : newContractRewardsUnpausedBlockNum;
									return ((tsRewardEnd - legacyContractRewardsPausedBlockNum)/numBlockPerDay)
								},
								dailyNumFish = 10,
								checkTokenEligibility = async ({staking_status:tokenStakingStatus} = {}) => {
									let token = false;
									try{
										const latestLegacyStakingTransferEvent = tokenStakingStatus.latest_stake_event,
											latestLegacyUnStakingTransferEvent = tokenStakingStatus.latest_unstake_event;
										if(
											(typeof latestLegacyStakingTransferEvent.blockNumber === "number" && latestLegacyStakingTransferEvent.blockNumber <= legacyContractRewardsPausedBlockNum)&&//user was staked before the rewards were paused
											(typeof latestLegacyUnStakingTransferEvent.blockNumber === "number" && latestLegacyUnStakingTransferEvent.blockNumber >= legacyContractRewardsPausedBlockNum)//user's latest unstake event from the contract is AFTER the rewards were paused
										){
											token = latestLegacyStakingTransferEvent;
										}
									} catch(e){
										console.error(`${loggingTag} Error`, e);
									}
									return token;
								};
							
							for(let j=0; j < tokensOwned.length; j++){
								const tokenID = tokensOwned[j];
								// console.info(`${loggingTag}[tokenID:${tokenID}] legacy wallet`, legacyWallet);
								// console.info(`${loggingTag}[tokenID:${tokenID}] staking status map`, legacyWallet.token_staking_status_map);
								const stakingStatusForThisToken = {
										legacy:legacyWallet.token_staking_status_map[tokenID],
										new: typeof newContractStakingWallet === "object" ? newContractStakingWallet.token_staking_status_map[tokenID] : {}
									},
									tokenIsEligible = await checkTokenEligibility({staking_status:stakingStatusForThisToken.legacy});
								
								if(tokenIsEligible){
									rewards = rewards + (dailyNumFish * rewardsRatePerFish({newContractStaking: stakingStatusForThisToken.new}));
								}
								
							}
							
						} catch(e){
							console.error(`${loggingTag} Error:`, e);
						}
						return rewards;
					}
					
					// console.info(`${loggingTag} rewards:`, await calculateRewards({
					// 	legacy_wallet:wallet,
					// 	new_wallet: newStakingContractWalletAddressMap[wallet.address]
					// }));
					
					await legacyContractWalletCollection.update({
						address: wallet.address
					},{
						$set:{
							// num_fish_rewards: (dailyNumFish * await numEligibleTokens({wallet}) * rewardsRatePerFish)
							num_fish_rewards: await calculateRewards({
								legacy_wallet:wallet,
								new_wallet: newStakingContractWalletAddressMap[wallet.address]
							})
						}
					});
				}
			// }
			
		} catch(e){
			console.error(`${loggingTag} Error querying events`, e);
		} finally {
			db.close({client: dbClient});
		}
	} catch(e){
		console.error(`${loggingTag} Error:`, e);
	}
}

const rebuildWalletsCollection = async ({events = [], address, collection_name:collectionName = "wallets", staking_wallet_address:stakingWalletAddr = ""} = {}) => {
	const loggingTag = `${appLoggingTag}[rebuildWalletsCollection]`;
	try{
		let tokenTransactionHistory = buildTokenTransactionHistoryMap({events});
		
		console.info(`${loggingTag} num events: ${events.length}`);
		
		//for debugging purposes
		
		//generating list of random IDs to spot check the owner for
		// const idsToSpotCheck = generateListOfIDs({num: 5});
		
		// for(let j =0; j < idsToSpotCheck.length; j++){
		// 	const tokenIDToRetrieveOwnerOf = idsToSpotCheck[j];
		// 	console.info(`${loggingTag} current owner of tokenID: ${tokenIDToRetrieveOwnerOf}`, addressOfCurrentOwner(tokenIDToRetrieveOwnerOf));
		// 	console.info(`${loggingTag} timestamp of latest transaction of tokenID: ${tokenIDToRetrieveOwnerOf}`, await getTimestampOfLatestTransaction(tokenIDToRetrieveOwnerOf));
		// }
		
		//for debugging purposes
		
		let tokenOwners = await buildTokenOwnerMap({src: tokenTransactionHistory, address, staking_wallet_address:stakingWalletAddr});
		console.info(`${loggingTag} token owners length`, tokenOwners.length);
		
		try {
			console.info(`${loggingTag} num owners:`, tokenOwners.length);
			await addWalletSummaryDocuments({owners: tokenOwners, collectionName, staking_wallet_address:stakingWalletAddr});
		} catch (e) {
			console.error(`${loggingTag} Error adding wallet summary events:`, e);
		}
	} catch(e){
		throw e;
	} finally {
	
	}
}

const printWalletRewardsResults = async ({} = {}) => {
	const loggingTag = `${appLoggingTag}[printWalletRewardsResults]`;
	const dbClient = await db.init({name: db.name});
	try{
		try{
			
			let legacyContractWalletCollection = await db.collection.init({client: dbClient, collection: {name: db.collection.names.legacy_contract_wallets}});
			const legacyWalletCursor = await legacyContractWalletCollection.find({num_fish_rewards:{$gt:0}}).sort({num_fish_rewards: -1});
			let walletsJSONArray = [];
			while(await legacyWalletCursor.hasNext()) {
				const wallet = await legacyWalletCursor.next();
				walletsJSONArray.push(wallet);
			}
			
			try{
				const data = JSON.stringify(walletsJSONArray);
				await fs.writeFileSync(`./rewards-list-${new Date().getTime()}.json`, data);
				console.info(`${loggingTag} successfully wrote json to disk!`);
			} catch(e){
				console.error(`${loggingTag} error writing data to json file`, e);
			}
			
		} catch(e){
			console.error(`${loggingTag} Error querying events`, e);
		} finally {
			db.close({client: dbClient});
		}
	} catch(e){
		console.error(`${loggingTag} Error:`, e);
	}
}

const buildTokenOwnerMap = async ({src:tokenOwnershipMapObj, address = '', staking_wallet_address:stakingWalletAddr = ''} = {}) => {
	const loggingTag = `${appLoggingTag}[buildTokenOwnerMap]`
	try{
		let tokenOwnerWalletMap = {};
		const tokenIDs = Object.keys(tokenOwnershipMapObj);
		//mapping all tokens to each wallet that owns it, and displaying the ts_oldest and ts_newest of each token owned per wallet
		for(let i = 0; i < tokenIDs.length; i++){
			const id = tokenIDs[i];
			try{
				
				const walletAddressOfOwner = addressOfCurrentOwner({id, map:tokenOwnershipMapObj, staking_wallet_address:stakingWalletAddr});
				
				const lastTransaction = getLatestTransferEventForTokenID({id, map: tokenOwnershipMapObj}),
					tsLastTransaction = lastTransaction.timestamp;
				
				console.info(`${loggingTag} current owner of tokenID: ${id}`, walletAddressOfOwner);
				if(!(walletAddressOfOwner in tokenOwnerWalletMap)){
					tokenOwnerWalletMap[walletAddressOfOwner] = {
						address: walletAddressOfOwner,
						contract_address: address,
						ts_oldest_token: tsLastTransaction,
						ts_latest_token: tsLastTransaction,
						tokens_owned: [id],
						num_tokens: 1
					}
				} else {//this wallet address has multiple tokens!
					let existingWalletMapping = tokenOwnerWalletMap[walletAddressOfOwner];
					console.info(`Already found wallet address: [${walletAddressOfOwner}] in map.`)
					existingWalletMapping.num_tokens++;
					existingWalletMapping.tokens_owned.push(id);
					if(tsLastTransaction < existingWalletMapping.ts_oldest_token){
						existingWalletMapping.ts_oldest_token = tsLastTransaction;
					} else if (tsLastTransaction > existingWalletMapping.ts_latest_token){
						existingWalletMapping.ts_latest_token = tsLastTransaction;
					}
					// console.info(`Updated wallet details:`, tokenOwnerWalletMap[walletAddressOfOwner]);
				}
				
			} catch(e){
				console.error(`Error occurred while processing token w/ id: "${id}"`, e);
			}
			
		}
		
		return tokenOwnerWalletMap;
	} catch(e){
		throw e;
	}
}

const addressOfCurrentOwner = ({id, map, staking_wallet_address:stakingWalletAddress = "0xb2F43262FC23d253538ca5F7b4890f89F0EE95D9"} = {}) => {
	const loggingTag = `${appLoggingTag}[addressOfCurrentOwner][wallet: ${stakingWalletAddress}]`;
	try{
		const ownershipHistory = map[id],
			numTransfersOfToken = ownershipHistory.length,
			latestTransfer = ownershipHistory[numTransfersOfToken - 1],
			fromAddress = "from" in latestTransfer.returnValues ? latestTransfer.returnValues.from : "",
			toAddress = "to" in latestTransfer.returnValues ? latestTransfer.returnValues.to : "",
			address = toAddress.toLowerCase() === stakingWalletAddress.toLowerCase() ? fromAddress : toAddress;//if the user is transferring TO the contract then set the owner of the token to be the last wallet that staked
		
		//debugging purposes
		if(address === "0xb2F43262FC23d253538ca5F7b4890f89F0EE95D9"){//legacy staking contract
			console.info(`${loggingTag} address set is the "${address === toAddress ? "to" : "from"}" address`, toAddress);
		} else if (address === "0xd09656a2EE7E5Ee3404fAce234e683D3337dA014"){//new staking contract
			console.info(`${loggingTag} address set is the "${address === toAddress ? "to" : "from"}" address`, toAddress);
		}
		
		if(typeof address !== "string"){
			console.info(`${loggingTag}[address: ${address}] address not found! latest transfer`, latestTransfer);
		}
		
		return address;
	} catch(e){
		console.error(`${loggingTag} Error:`, e);
		throw e;
	}
}

const getLatestTransferEventForTokenID = ({id, map} = {}) => {
	const loggingTag = `${appLoggingTag}[getTransactionOfLastTransfer]`;
	
	try{
		const ownershipHistory = map[id],
			numTransfersOfToken = ownershipHistory.length,
			latestTransfer = ownershipHistory[numTransfersOfToken - 1];
		console.info(`${loggingTag} latest transfer`, latestTransfer);
		return latestTransfer;
		// don't need to read from blockchain again 9.30.21 -d
		// return await web3.eth.getTransaction(latestTransfer.transactionHash);
		// console.info(`${loggingTag} latest transaction of token w/ ID ${id}`, transaction);
	} catch(e){
		console.error(`${loggingTag} Error getting transaction`, e);
		throw e;
	}
}

const addWalletSummaryDocuments = async ({owners = [], collectionName = "wallets", staking_wallet_address:stakingWalletAddr} = {}) => {
	const loggingTag = `${appLoggingTag}[addWalletSummaryDocuments]`;
	try{
		const ownersWalletIDs = Object.keys(owners);
		
		const dbClient = await db.init({name: db.name});//init mongodb and returns walletCollection AKA reference to a table
		const walletCollection = await db.collection.init({
			client:dbClient,
			collection:{
				name: collectionName
			}
		}),
			eventsCollection = await db.collection.init({
				client: dbClient,
				collection: {name: db.collection.names.mutantdao}
			});
		
		//truncating the walletCollection first
		await walletCollection.remove({});
		
		for(let l = 0; l < ownersWalletIDs.length; l++){
			const address = ownersWalletIDs[l];
			let wallet = owners[address];
			
			try{
				wallet = await updateTokenOwnersMapWithStakingEventMap({
					wallet,
					staking_wallet_address:stakingWalletAddr,
					dbClient,
					collection: eventsCollection
				});
			} catch(e){
				console.error(`${loggingTag} Error:`, e);
			}
			
			try{
				await db.collection.upsert({
					id: address,
					document:owners[address],
					collection: walletCollection
				});
			} catch(e){
				console.error(`${loggingTag} Error unable to insert info for wallet address: "${address}" rows:`, e);
			}
			
			console.info(`owner wallet details:`, owners[address]);
		}
		
		db.close({client: dbClient});
	} catch(e){
		throw e;
	}
}

const buildTokenTransactionHistoryMap = ({events = []} = {}) => {
	const loggingTag = `${appLoggingTag}[buildTokenTransactionHistoryMap]`;
	try{
		let tokenOwnershipMapObj = {};
		
		//building a map of each token in the collection, and the transfers of ownership 9.21.21 KL
		for(let i = 0; i < events.length; i++){
			
			const eventInfo = events[i];
			
			// console.info(`${loggingTag} return values`, eventInfo.returnValues);
			const details = eventInfo.returnValues,
				tokenIDs = "tokenId" in details ? [details.tokenId] : "tokens" in details ? details.tokens.split(",") : -999;
			
			//note a redemption make include multiple tokens so I had to edit the following logic
			if(tokenIDs !== -999){
				for(let j=0; j < tokenIDs.length; j++){
					// console.info(`${loggingTag} Event info`, eventInfo);
					const tokenID = tokenIDs[j];
					if(tokenID.length > 0){//example of tokens property of redemptioned: "1234,"   because of the split, I will be returned TWO items.   Therefore I need to check the length of
						//each individual item
						if(!(tokenID in tokenOwnershipMapObj)){
							// console.info(`${loggingTag} new event type found! pushing type ("${tokenID}") to array!`);
							tokenOwnershipMapObj[tokenID] = [eventInfo];
						} else {//token already in the
							tokenOwnershipMapObj[tokenID].push(eventInfo);
						}
						// if(tokenID === "579"){
						// 	console.info(`${loggingTag} tokenID `, tokenOwnershipMapObj[tokenID]);
						// }
					}
				}
			}
			
		}
		return tokenOwnershipMapObj;
	} catch(e){
		console.error(`${loggingTag} Error`, e);
		throw e;
	}
}

const getLatestStakingTransferOfTokenFromAnAddress = async ({token_id:tokenID = "", wallet_address:walletAddress = "", staking_wallet_address:stakingWalletAddress = "", collection} = {}) => {
	const loggingTag = `${appLoggingTag}[getLatestStakingTransferOfTokenFromAnAddress]`;
	let transfer = [];
	try{
		// const dbClient = await db.init({name: db.name});
		// try {
		// 	const collection = await db.collection.init({client: dbClient, collection: {name: db.collection.names.mutantdao}});
			// example event
			// {event: "Transfer", "returnValues.from":"0x35f80420bbDB358b6bf274038aeD03B49235E9fC", "returnValues.to":"0xd09656a2EE7E5Ee3404fAce234e683D3337dA014"}
			const cursor = await collection.find({event: "Transfer", "returnValues.from":walletAddress, "returnValues.to":stakingWalletAddress}).sort({blockNumber: -1}).limit(1);
			while(await cursor.hasNext()){
				transfer = await cursor.next()
				// log.info(`${loggingTag}[count:${await cursor.count()}] event:`, sale);
			}
		// } catch(e){
		// 	log.error(`${loggingTag} Error:`, e);
		// } finally {
		// 	db.close({client: dbClient});
		// }
	} catch(e){
		log.error(`${loggingTag} Error init-ing db client:`, e);
		throw e;
	}
	return transfer;
}

const getLatestUnstakeTransferOfTokenFromAnAddress = async ({token_id:tokenID = "", wallet_address:walletAddress = "", staking_wallet_address:stakingWalletAddress = "", collection} = {}) => {
	const loggingTag = `${appLoggingTag}[getLatestUnstakeTransferOfTokenFromAnAddress]`;
	let transfer = [];
	try{
		// const dbClient = await db.init({name: db.name});
		// try {
		// 	const collection = await db.collection.init({client: dbClient, collection: {name: db.collection.names.mutantdao}});
			// example event
			// {event: "Transfer", "returnValues.from":"0x35f80420bbDB358b6bf274038aeD03B49235E9fC", "returnValues.to":"0xd09656a2EE7E5Ee3404fAce234e683D3337dA014"}
			const cursor = await collection.find({event: "Transfer", "returnValues.from":stakingWalletAddress, "returnValues.to":walletAddress}).sort({blockNumber: -1}).limit(1);
			while(await cursor.hasNext()){
				transfer = await cursor.next()
				// log.info(`${loggingTag}[count:${await cursor.count()}] event:`, sale);
			}
		// } catch(e){
		// 	log.error(`${loggingTag} Error:`, e);
		// } finally {
		// 	db.close({client: dbClient});
		// }
	} catch(e){
		log.error(`${loggingTag} Error init-ing db client:`, e);
		throw e;
	}
	return transfer;
}

const updateTokenOwnersMapWithStakingEventMap = async ({wallet = {}, staking_wallet_address:stakingWalletAddress = "", dbClient, collection} = {}) => {
	const loggingTag = `${appLoggingTag}[updateTokenOwnersMapWithStakingEventMap]`;
	let updatedWallet = {}
	try{
			const ownerTokens = wallet.tokens_owned;
			
			wallet.token_staking_status_map = {};
		
		// const dbClient = await db.init({name: db.name});
		// try {
		// 	const collection = await db.collection.init({
		// 		client: dbClient,
		// 		collection: {name: db.collection.names.mutantdao}
		// 	});
		//
			for (let j = 0; j < ownerTokens.length; j++) {
				const tokenID = ownerTokens[j];
				const latestStakingTransferOfToken = await getLatestStakingTransferOfTokenFromAnAddress({
					token_id: tokenID,
					wallet_address: wallet.address,
					staking_wallet_address: stakingWalletAddress,
					collection
				});
				const latestUnstakingTransferOfToken = await getLatestUnstakeTransferOfTokenFromAnAddress({
					token_id: tokenID,
					wallet_address: wallet.address,
					staking_wallet_address: stakingWalletAddress,
					collection
				});
				wallet.token_staking_status_map[tokenID] = {
					latest_stake_event: latestStakingTransferOfToken,
					latest_unstake_event: latestUnstakingTransferOfToken
				};
			}
		// } catch(e){
		// 	console.error(`${loggingTag} Error:`, e);
		// } finally {
		// 	await db.close({client:dbClient});
		// }
			updatedWallet = wallet;
	} catch(e){
		console.error(`${loggingTag} Error`, e);
		throw e;
	}
	return updatedWallet;
}
const rebuildLegacyWalletProfiles = async ({} ={}) => {
	await rebuildWalletProfiles({staking_wallet_address:"0xb2F43262FC23d253538ca5F7b4890f89F0EE95D9"});
}

const rebuildNewWalletProfiles = async ({} = {}) => {
	await rebuildWalletProfiles({is_legacy: false, staking_wallet_address: "0xd09656a2EE7E5Ee3404fAce234e683D3337dA014"});//recursively call itself
}

module.exports = {
	start: getAllEvents,
	rebuildLegacyWalletProfiles,
	rebuildNewWalletProfiles,
	calculateFishRewardsForWallets,
	printWalletRewardsResults,
}