import * as fs from "fs";
import * as requestPromise from "request-promise-native";

const FACEAPI_KEY = "1ca458bc00bf409fae1c43db9daa4b49";
const FACEAPI_ENDPOINT = "https://southeastasia.api.cognitive.microsoft.com/face/v1.0";
const PERSON_GROUP_ID = 'mstc-ai-readiness-ericsk';
const PERSON_GROUP_NAME = 'MSTC AI Readiness Group';

async function getFaceIdAsync(path: string): Promise<string> {
    let img = fs.readFileSync(path);
    let reqOpt = {
        url: `${FACEAPI_ENDPOINT}/detect?returnFaceLandmarks=false`,
        method: 'POST',
        headers: {
            'Content-Type': 'application/octet-stream',
            'Ocp-Apim-Subscription-Key': FACEAPI_KEY
        },
        body: img
    };
    try {
        const response = await requestPromise(reqOpt);
        let faces = JSON.parse(response);
        return Promise.resolve(faces[0].faceId);
    } catch (errors) {
        return Promise.reject(errors);
    }
}

async function identifyFaceAsync(faceId: string): Promise<string> {
    let reqOpt = {
        url: `${FACEAPI_ENDPOINT}/identify`,
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Ocp-Apim-Subscription-Key': FACEAPI_KEY
        },
        body: {
            'personGroupId': PERSON_GROUP_ID,
            'faceIds': [
                faceId
            ]
        },
        json: true
    };
    try {
        const response = await requestPromise(reqOpt);
        return Promise.resolve(response[0].candidates[0].personId);
    } catch (errors) {
        return Promise.reject(errors);
    }

}

async function getPersonAsync(personId: string): Promise<any> {
    let reqOpt = {
        url: `${FACEAPI_ENDPOINT}/persongroups/${PERSON_GROUP_ID}/persons/${personId}`,
        method: 'GET',
        json: true,
        headers: {
            'Content-Type': 'application/json',
            'Ocp-Apim-Subscription-Key': FACEAPI_KEY
        }
    };

    try {
        const response = await requestPromise(reqOpt);
        return Promise.resolve(response);
    } catch (errors) {
        return Promise.reject(errors);
    }
}

/**
 * Main entry.
 */
async function main() {
    const pathToLab: string = "./test.jpg";
    let content = fs.readFileSync(pathToLab);
    
    console.debug("\u001b[1;37mDetecting face id...\u001b[0m");
    let faceId = await getFaceIdAsync(pathToLab);
    console.debug(`\u001b[1;37mFace id: ${faceId}...\u001b[0m`);
    let personId = await identifyFaceAsync(faceId);
    console.debug(`\u001b[1;37mPerson id: ${personId}...\u001b[0m`);
    let person = await getPersonAsync(personId);
    console.debug(`\u001b[1;37mPerson: ${person.name}...\u001b[0m`);
    console.info("\u001b[1;32mDone.\u001b[0m");
}

main();