import * as fs from "fs";
import * as requestPromise from "request-promise-native";

const FACEAPI_KEY = ""; // Get API Key on Azure Portal.
const FACEAPI_ENDPOINT = "https://API_LOCATION_HERE.api.cognitive.microsoft.com/face/v1.0";
const PERSON_GROUP_ID = 'mstc-ai-readiness-ericsk';
const PERSON_GROUP_NAME = 'MSTC AI Readiness Group';

/**
 * Create a person group.
 * 
 * @returns {Promise<string>} The PersonGroup ID
 */
async function createPersonGroupAsync(): Promise<string> {
    console.debug(`\u001b[1;37mCreating person group ${PERSON_GROUP_ID}...\u001b[0m`);

    let requestOption: any = {
        url: `${FACEAPI_ENDPOINT}/persongroups/${PERSON_GROUP_ID}`,
        method: 'PUT',
        body: {
            'name': PERSON_GROUP_NAME
        },
        headers: {
            'Content-Type': 'application/json',
            'Ocp-Apim-Subscription-Key': FACEAPI_KEY
        },
        json: true
    };

    try {
        const resp = await requestPromise(requestOption);
        console.info(`\u001b[1;32mPerson Group ${PERSON_GROUP_ID} has been created.\u001b[0m`);
        return Promise.resolve(PERSON_GROUP_ID);
    } catch (error) {
        return Promise.reject(error);
    }
}

/**
 * Create a person under a person group.
 * 
 * @param {string} personGroupId The PersonGroup ID.
 * @param {any} data The metadata attached to the person.
 * @returns {Promise<string>} Person ID
 */
async function createPersonAsync(personGroupdId: string, name: string, data: any): Promise<string> {
    let requestOption: any = {
        url: `${FACEAPI_ENDPOINT}/persongroups/${personGroupdId}/persons`,
        method: 'post',
        json: true,
        headers: {
            'Ocp-Apim-Subscription-Key': FACEAPI_KEY
        },
        body: {
            'name': name,
            'userData': JSON.stringify(data)
        }
    };

    try {
        const resp = await requestPromise(requestOption);
        return Promise.resolve(resp.personId);
    } catch (errors) {
        return Promise.reject(errors);
    }
}

/**
 * Add a face image to a person.
 * 
 * @param {string} personGroupId The person group ID.
 * @param {string} personId The person ID.
 * @param {string} faceImageUrl The face image URL.
 * @returns {Promise<string>} The persisted face ID.
 */
async function addPersonFace(personGroupId: string, personId: string, faceImageUrl: string) : Promise<string> {
    let reqAdd = {
        url: `${FACEAPI_ENDPOINT}/persongroups/${personGroupId}/persons/${personId}/persistedFaces`,
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Ocp-Apim-Subscription-Key': FACEAPI_KEY
        },
        body: {
            'url': faceImageUrl
        },
        json: true
    };

    try {
        const response = await requestPromise(reqAdd);
        return Promise.resolve(response.persistedFaceId);
    } catch (errors) {
        return Promise.reject(errors);
    }
}

/**
 * Train the person group.
 * 
 * @param {string} personGroupId The person group ID.
 */
async function trainPersonGroup(personGroupId: string): Promise<void> {
    let requestOption = {
        url: `${FACEAPI_ENDPOINT}/persongroups/${personGroupId}/train`,
        method: 'POST',
        headers: {
            'Ocp-Apim-Subscription-Key': FACEAPI_KEY
        }
    };

    try {
        const response = await requestPromise(requestOption);
        return Promise.resolve();
    } catch (errors) {
        return Promise.reject(errors);
    }
}

/**
 * Main entry.
 */
async function main() {
    const pathToLab: string = "./lab.json";
    let content = fs.readFileSync(pathToLab, { encoding: "UTF-8" });
    let people: Array<any> = JSON.parse(content);
    
    let personGroupId = await createPersonGroupAsync();
    for (let i = 0; i < people.length; ++i) {
        let p = people[i];

        let personId = await createPersonAsync(personGroupId, p.name, {});
        console.debug(`\u001b[1;37mPerson: ${personId} / ${p.name}\u001b[0m`);
        for (let j = 0; j < p.faces.length; ++j) {
            let pFaceId = await addPersonFace(personGroupId, personId, p.faces[j]);
            console.debug(`\u001b[1;30m  added Face ID: ${pFaceId}\u001b[0m`);
        }
    }
    console.debug("\u001b[1;37mTraining person group...\u001b[0m");
    await trainPersonGroup(personGroupId);

    console.info("\u001b[1;32mDone.\u001b[0m");
}

main();