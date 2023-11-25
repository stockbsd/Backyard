#!python3
import sys
import urllib.request, json

url = "http://183.235.16.92:8082/epg/api/custom/getAllChannel2.json"
response = urllib.request.urlopen(url)
data = json.loads(response.read())

#with open('1.txt') as f:
#    data = json.loads(f.read())

surl, burl = 'hwurl', 'zteurl' 
#print(data)
cctv, high, migu, others = [],[],[],[]
for ch in data['channels']:
    if not (ch['params'][surl]):
        if (ch['params'][burl]):
            ch['params'][surl] = ch['params'][burl]
        else:
            continue

    if "CCTV" in ch['title']:
        cctv.append(ch)
    elif "咪咕-" in ch['title']:
        migu.append(ch)
    elif "高清" in ch['title']:
        high.append(ch)
    else:
        others.append(ch)

channels = cctv + migu + high + others

print('#EXTM3U')
for ch in channels:
        print('#EXTINF:-1,', ch['title'])
        print(ch['params'][surl])

print(len(channels), file=sys.stderr)
