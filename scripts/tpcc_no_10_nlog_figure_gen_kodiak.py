#! /usr/bin/env python

import figure

figure.xxx = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 40, 50, 60, 70, 80, 90, 100]

if __name__ == "__main__":

    cpu = [
            [0.484146248949, 0.745739428283, 0.889002267574, 0.949792099792, 0.968766276049, 0.985086076389, 0.991115160816, 0.99459550189, 0.996466625363, 0.998025250457, 0.9977872887, 0.99744666364, 0.996936228069, 0.995290770382, 0.994382660009, 0.991884183404, 0.986154804797, 0.980840552358, 0.975846763072, 0.96650135117, 0.936167800454, 0.93735832804, 0.923129251701, 0.910657596372, 0.999688279302, 0.942452652567, 1.00011340441, 0.864325348272], # occ
            [0.527237815646, 0.7970871032, 0.918868394303, 0.959574102685, 0.976092685818, 0.978796447032, 0.98337239023, 0.986954953227, 0.987006237006, 0.989450749426, 0.985986693817, 0.988878801634, 0.98320665167, 0.983375921549, 0.983208531017, 0.985985020427, 0.98904966767, 0.986496485777, 0.983658647299, 0.990282242246, 0.990543046403, 0.990906310108, 0.991686334154, 0.991476696267, 0.995510101991, 0.996306268717, 0.996162885448, 0.996930050575], # rococo
            #[__2pl.cpu__], # 2pl timeout
            #[__2pl_wait_die.cpu__], # 2pl wait die
            [0.51189562548, 0.773425521614, 0.892797653303, 0.943820861678, 0.968667468543, 0.981010303789, 0.988047271807, 0.992049631638, 0.994335086573, 0.995998050612, 0.996822514753, 0.997787108488, 0.998127965248, 0.99897861392, 0.998921924648, 0.999262420985, 0.999319342004, 0.999489332728, 0.999829777576, 0.999886595594, 0.999546485261, 0.998545173795, 0.997869022869, 0.997052513964, 0.998232588754, 0.997557172557, 0.997400642035, 0.997868828287] # 2pl wound wait
            ]

    flush_cnt = [
            #[-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0], # occ
            #[-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0], # rococo
            #[__2pl.flush_cnt__], # 2pl timeout
            #[__2pl_wait_die.flush_cnt__], # 2pl wait die
            #[-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0] # 2pl wound wait
            ]

    flush_sz = [
            #[-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0], # occ
            #[-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0], # rococo
            #[__2pl.flush_sz__], # 2pl timeout
            #[__2pl_wait_die.flush_sz__], # 2pl wait die
            #[-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0] # 2pl wound wait
            ]

    throughput_val = [
            [4644, 7963, 9902, 10697, 10779, 10803, 10636, 10471, 10297, 10150, 9059, 8219, 7551, 6949, 6444, 6038, 5672, 5320, 5007, 4767, 2892, 2068, 1592, 1264, 1405, 1062, 1095, 649], # occ
            [5296, 8702, 10387, 10886, 11035, 10989, 10980, 10987, 10953, 10976, 11018, 11010, 10964, 10921, 10845, 10861, 10898, 10813, 10724, 10614, 10393, 10369, 10396, 10312, 10341, 10374, 10281, 10266], # rococo
            #[__2pl.tps__], # 2pl timeout
            #[__2pl_wait_die.tps__], # 2pl wait die
            [4356, 7126, 8223, 8541, 8668, 8628, 8568, 8546, 8490, 8409, 8330, 8193, 8162, 8071, 7969, 7934, 7818, 7700, 7634, 7477, 6453, 5239, 4032, 3221, 2624, 2188, 1881, 1637] # 2pl wound wait
            ]

    commit_rate_val = [
            [123012.0/123641, 178892.0/180786, 231754.0/235577, 256014.0/261628, 262720.0/269980, 262112.0/270816, 255872.0/265886, 251520.0/262801, 246740.0/259253, 243543.0/257434, 196961.0/229541, 177629.0/225783, 162762.0/223607, 149250.0/220108, 137901.0/217509, 129040.0/215245, 121169.0/212510, 113355.0/208771, 106566.0/205627, 107748.0/216380, 64733.0/198715, 46353.0/196007, 35872.0/189976, 28731.0/188174, 32778.0/229971, 24838.0/206939, 25940.0/230616, 15448.0/178204],
            [113110.0/113110, 196261.0/196261, 242724.0/242724, 257332.0/257332, 262439.0/262439, 261052.0/261052, 262179.0/262179, 261460.0/261460, 261279.0/261279, 262544.0/262544, 242131.0/242131, 242166.0/242166, 241262.0/241262, 241045.0/241045, 238133.0/238133, 238939.0/238939, 240240.0/240240, 239389.0/239389, 236840.0/236840, 255589.0/255589, 252043.0/252043, 255073.0/255073, 195158.0/195158, 196153.0/196153, 199354.0/199354, 268890.0/268890, 268476.0/268476, 271959.0/271959],
            #[__2pl.commit_rate__],
            #[__2pl_wait_die.commit_rate__],
            [114998.0/115215, 157432.0/157975, 185695.0/186669, 194615.0/195923, 198270.0/200036, 198046.0/200009, 196525.0/198942, 196247.0/198916, 194719.0/197590, 193142.0/196324, 179896.0/183319, 176619.0/180623, 175729.0/180486, 173895.0/179821, 171862.0/178784, 170932.0/178702, 168274.0/177264, 165797.0/176274, 164340.0/176341, 171398.0/186037, 148557.0/189602, 120872.0/196185, 93235.0/205986, 74913.0/214220, 61505.0/221474, 51522.0/226638, 44660.0/230824, 39299.0/235856]
            ]

    latency_50_val = [
            [1.54938752561, 1.80542715771, 2.16959385679, 2.66604469331, 3.27132222853, 3.92381911429, 4.62608264124, 5.35221888418, 6.09209124483, 6.83991533553, 7.62233678475, 8.34770292904, 8.95823881925, 9.52745751544, 10.0704005606, 10.460863028, 10.6993505603, 10.938146882, 11.3619756431, 11.476643785, 17.1031619432, 23.652747094, 30.1830005629, 36.0403242303, 69.583430355, 62.807065114, 131.020206881, 64.7721815855],
            [1.3298983964, 1.6049595975, 2.01545918989, 2.57499606615, 3.19386836985, 3.84833319472, 4.51631559043, 5.17449625383, 5.85416451628, 6.50979776619, 7.11790236932, 7.79945375312, 8.44341428038, 9.13196549883, 9.85472462612, 10.5434660433, 11.220124602, 11.9392066897, 12.6788877941, 13.582483665, 21.1914290792, 28.8807398335, 36.3723872207, 44.0264092965, 51.6301392952, 59.0715081798, 67.27565906, 75.07081343],
            #[__2pl.latency_50__],
            #[__2pl_wait_die.latency_50__],
            [1.653182929, 2.01211794269, 2.60749177221, 3.34696591865, 4.12526203713, 4.97029965675, 5.83229236533, 6.69318530696, 7.56968166985, 8.495595053, 9.13638590252, 9.86630068528, 10.4950968702, 11.2431956011, 12.1101989146, 12.9897115577, 14.0550715492, 15.2015026125, 16.2725387642, 17.5649118327, 29.2883912365, 46.1583258295, 73.089001175, 108.997592331, 162.805759751, 226.407581339, 298.491623484, 390.461375174]
            ]

    latency_90_val = [
            [1.66917563738, 1.93969272861, 2.33161222609, 2.86745957971, 3.52064558249, 4.22091658711, 4.9816209639, 5.7593626368, 6.55842015164, 7.35840028202, 8.23656637917, 9.08234956468, 9.886016673, 10.7349234454, 11.5891575203, 12.4448458125, 13.339875209, 14.2756816845, 15.46582786, 16.6064900178, 33.8108634587, 57.0597349964, 92.800460806, 135.261051525, 226.492400904, 261.236147155, 390.729769269, 424.313423372],
            [1.44667705805, 1.74283459415, 2.1921955599, 2.79346508712, 3.45933156851, 4.16112826005, 4.87748770338, 5.58696123185, 6.31080189211, 7.0150610368, 7.66440553731, 8.3936117088, 9.08241478246, 9.82303814765, 10.5982423577, 11.3284313131, 12.0414310715, 12.8197628793, 13.6097330743, 14.5765795505, 22.4438501242, 30.0946572005, 37.6059465924, 45.502997998, 53.1664587339, 60.736090104, 68.9700269591, 76.8694328251],
            #[__2pl.latency_90__],
            #[__2pl_wait_die.latency_90__],
            [1.78323646794, 2.17672745172, 2.82729340115, 3.62991733589, 4.46541887594, 5.37972752437, 6.30861252654, 7.22154613599, 8.16968711706, 9.15446696037, 9.96631104864, 10.9832785259, 11.9243222691, 12.9604202036, 14.0596986799, 15.0573410943, 16.2107487805, 17.3768033278, 18.4541009749, 19.7661452308, 34.1698722986, 56.0509615434, 91.022174862, 136.99456944, 198.282840641, 272.681651255, 358.088898924, 460.666042207]
            ]

    latency_99_val = [
            [1.70355821073, 1.98189416385, 2.38990403838, 2.94988094652, 3.6475689118, 4.37960142559, 5.18805056042, 6.02170847092, 6.88421550003, 7.75673577389, 8.83447477975, 10.1870515597, 11.7958218744, 13.7375285245, 15.8140307981, 17.9287997901, 20.351524376, 22.9341661048, 25.776818084, 28.5840777513, 69.9161777098, 129.960240319, 213.238232014, 317.403140145, 360.311765772, 502.454539465, 593.652313175, 1073.35382619],
            [1.48124904156, 1.78285294483, 2.24340471723, 2.8571319955, 3.53732640507, 4.25395185851, 4.98509491559, 5.70933442952, 6.44437663799, 7.16365438225, 7.82208446153, 8.56405088727, 9.26400553695, 10.0172681975, 10.8059001276, 11.5450798534, 12.2661904702, 13.0557158863, 13.8521305871, 14.8387548463, 22.7561248504, 30.4259140123, 37.9478515519, 45.9339545807, 53.6197643588, 61.204859509, 69.4870272488, 77.3893577845],
            #[__2pl.latency_99__],
            #[__2pl_wait_die.latency_99__],
            [1.82070344281, 2.22459953932, 2.89079131199, 3.70943536255, 4.56384197306, 5.50064074135, 6.45919546311, 7.39892755308, 8.37874780955, 9.39788427333, 10.4340029659, 11.5649945623, 12.5681707084, 13.6809488512, 14.8477107567, 15.9079172897, 17.1529460552, 18.434246877, 19.6206046284, 21.0840836001, 36.6601249968, 60.1993897135, 97.8157782059, 146.995839365, 210.922722971, 289.058800781, 378.937329864, 484.177280971]
            ]

    latency_999_val = [
            [1.71342380748, 1.99932784246, 2.41285310642, 2.97913007792, 3.69375907031, 4.42675182574, 5.24617147088, 6.09147791659, 6.96876276996, 7.85517583423, 9.45324421404, 11.3304784542, 13.3440924326, 15.6451780606, 18.0632357234, 20.5183725337, 23.1913978919, 26.1167525114, 29.33532573, 32.441731975, 79.3005971743, 147.130098683, 240.447956646, 356.983621194, 394.240184004, 562.039107751, 645.264976709, 1197.08120954],
            [1.48619039024, 1.78901238533, 2.2511814664, 2.86677924913, 3.54909837114, 4.29035276715, 5.02211288638, 5.75118097295, 6.50249279871, 7.21680669253, 7.91052027578, 8.62819421531, 9.39865657929, 10.1526180915, 10.9670608655, 11.7097195063, 12.3933865583, 13.2060905038, 14.0657626556, 14.9733231219, 22.9820236075, 30.7289212776, 38.3507396458, 46.4141052168, 54.0362201889, 61.5702597721, 69.8991740645, 77.7783430716],
            #[__2pl.latency_999__],
            #[__2pl_wait_die.latency_999__],
            [1.8272957511, 2.23548974286, 2.90795485853, 3.73461869609, 4.60116851446, 5.54828905902, 6.51914460904, 7.46945363184, 8.45962266561, 9.49025943716, 10.5377230372, 11.6882691311, 12.7084353899, 13.8412827534, 15.0201923423, 16.0912221142, 17.3518067931, 18.6534660899, 19.8573451151, 21.344625807, 37.1130323699, 60.9430196978, 98.9830713728, 148.692945786, 213.031522058, 291.775471732, 382.487430334, 487.966939508]
            ]

    n_try_50_val = [
            [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.22112392458, 1.0845478702, 1.78874325366, 1.0],
            [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            #[__2pl.n_try_50__],
            #[__2pl_wait_die.n_try_50__],
            [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.21069566896, 1.56204613413, 2.12109781478, 2.70835759481, 3.21607702642, 3.82187388671]
            ]

    n_try_90_val = [
            [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.00035734227, 1.01390701797, 1.02628814493, 1.04190661336, 1.06031229477, 1.08754131521, 1.11454734823, 1.43413034896, 1.78136011698, 2.20502416057, 2.61693158526, 4.03098305085, 3.97633533148, 5.33110597104, 4.38358627634],
            [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            #[__2pl.n_try_90__],
            #[__2pl_wait_die.n_try_90__],
            [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.12138278696, 1.39077437859, 1.90600755562, 2.48461161953, 3.16918379882, 3.89937242554, 4.58598298253, 5.37623342475]
            ]

    n_try_99_val = [
            [1.0, 1.00049123956, 1.00620216531, 1.01138672653, 1.01667486889, 1.02192762727, 1.02723113303, 1.03249345392, 1.03743777428, 1.04312193342, 1.06178233867, 1.11214544048, 1.18399592885, 1.26802791069, 1.35482453249, 1.43120494094, 1.51555974224, 1.5933916112, 1.67592417062, 1.75096090747, 2.65227432316, 3.63943428708, 4.59400782812, 5.61635551805, 6.37103235747, 7.16678189434, 8.05654205607, 10.2639769829],
            [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            #[__2pl.n_try_99__],
            #[__2pl_wait_die.n_try_99__],
            [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.00209191042, 1.00324782277, 1.00439381442, 1.00599864024, 1.00850660034, 1.0119186512, 1.01612337688, 1.02253769837, 1.02867587853, 1.03351810048, 1.04086055069, 1.04968349996, 1.05822515612, 1.06981801466, 1.24949854152, 1.58304572006, 2.15746137678, 2.79456062996, 3.52643334592, 4.31123789358, 5.07434464976, 5.89456639079]
            ]

    n_try_999_val = [
            [1.00408502051, 1.00949567183, 1.01515622705, 1.02029269971, 1.02558895362, 1.03114772254, 1.03697342889, 1.04272330738, 1.0484232818, 1.05469401847, 1.13544652477, 1.23474085804, 1.33374744002, 1.43527162978, 1.53541952484, 1.62170506555, 1.70592414517, 1.78971397285, 1.87735184437, 1.9552211074, 2.96726356158, 4.07385651967, 5.12520928675, 6.25817016236, 6.94429683921, 7.92822310885, 8.72817781894, 11.3661871436],
            [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            #[__2pl.n_try_999__],
            #[__2pl_wait_die.n_try_999__],
            [1.00087045081, 1.00242888208, 1.00418308546, 1.00563728012, 1.00778003847, 1.00873907615, 1.01108349293, 1.01222647284, 1.01336595998, 1.0149522151, 1.01743862539, 1.02082270661, 1.02501238942, 1.03201685461, 1.03817345215, 1.04331199747, 1.05128342405, 1.06105137323, 1.07048576214, 1.08290796958, 1.27233707078, 1.61704664972, 2.20140432248, 2.8482722681, 3.58932669303, 4.38193122207, 5.15748066794, 5.98160931251]
            ]

    att_latency_50_val = [
            [1.55017085895, 1.80483033603, 2.16730666769, 2.66200673307, 3.26511254704, 3.91438931539, 4.61279844682, 5.33572952898, 6.06958810339, 6.8143607754, 7.59499122225, 8.33372074482, 8.99700204886, 9.67316739866, 10.3434311562, 10.9552717506, 11.5726678694, 12.3033120002, 13.1301342099, 13.8471576967, 23.1855114544, 32.0953436937, 42.0607962178, 52.1062169355, 53.1605957668, 61.578430095, 69.5407810461, 96.8921548012],
            [1.3315316974, 1.60659596665, 2.01709623685, 2.57663085163, 3.19550721675, 3.84997331897, 4.51795260222, 5.17613935964, 5.85580270606, 6.5114426709, 7.11955693352, 7.80110961584, 8.44507686972, 9.13362567817, 9.85638411433, 10.5451310832, 11.221787685, 11.9408721365, 12.6805527581, 13.5841486447, 21.1931202752, 28.8824549244, 36.3741082146, 44.0281886733, 51.6319842902, 59.0733207694, 67.277568685, 75.072777816],
            #[__2pl.att_latency_50__],
            #[__2pl_wait_die.att_latency_50__],
            [1.65421808141, 2.01292325683, 2.60732183997, 3.34576953526, 4.12266791977, 4.96575308619, 5.82612351615, 6.68429608448, 7.55906157479, 8.48249152927, 9.1178535589, 9.84050819929, 10.4615284933, 11.1868853934, 12.0151532011, 12.8420540928, 13.829897303, 14.8757799581, 15.8249412423, 16.9602303221, 22.14356623, 22.4618229117, 21.60799019, 23.1481533227, 25.5591483527, 28.9000935688, 32.3569319247, 36.1485596552]
            ]

    att_latency_90_val = [
            [1.66919020348, 1.93735341539, 2.32652413835, 2.85831322339, 3.50566591188, 4.20008210896, 4.95297163532, 5.72079620439, 6.5094651188, 7.2955995225, 8.15129047062, 8.97723602651, 9.77607581792, 10.6336506557, 11.4678335866, 12.314651689, 13.2054419495, 14.1590362992, 15.1354000392, 16.0436428616, 25.9446387083, 35.2101151159, 45.6493215798, 55.8467744283, 55.8883737038, 69.8856982318, 72.82733194, 102.865530855],
            [1.44831517619, 1.7444736041, 2.19383387737, 2.79510202725, 3.46097088864, 4.1627693463, 4.87912637915, 5.58860494648, 6.31244238555, 7.01670647863, 7.66606104349, 8.39526874178, 9.08408258219, 9.82469854361, 10.5999031083, 11.3300970028, 12.0430953008, 12.8214281122, 13.6114000525, 14.578244591, 22.4455423552, 30.0963682937, 37.6076663478, 45.5047681043, 53.1683056588, 60.7378923669, 68.9719420032, 76.871391501],
            #[__2pl.att_latency_90__],
            #[__2pl_wait_die.att_latency_90__],
            [1.78401937729, 2.17686844739, 2.82589598049, 3.62687546189, 4.45970333898, 5.37210553365, 6.29608717684, 7.2063471383, 8.14996066095, 9.12947933402, 9.91577749959, 10.8891919834, 11.7962512891, 12.7830131549, 13.8266014171, 14.7721902563, 15.8498983567, 16.9122911208, 17.8726499223, 19.0163461519, 27.6193975477, 34.3819791177, 38.8778176541, 42.9837932137, 47.2291547221, 51.5803037505, 56.3751289506, 61.2920341654]
            ]

    att_latency_99_val = [
            [1.70263231878, 1.97576558707, 2.37128406728, 2.9128249632, 3.57381251911, 4.27869160509, 5.04609970022, 5.82761420041, 6.63051075786, 7.42960347299, 8.305841486, 9.15551009963, 9.98557041339, 10.8820721166, 11.7524395843, 12.6479890435, 13.6055031957, 14.6231500816, 15.653895719, 16.6294991221, 26.8971174638, 36.4110743685, 47.214095368, 57.7164289329, 56.6954266055, 71.9168296015, 73.8200138988, 106.291934751],
            [1.48288809207, 1.78449224349, 2.24504379576, 2.85876952535, 3.53896596159, 4.25559349287, 4.98673414846, 5.71097869581, 6.44601757585, 7.16530046368, 7.82374013114, 8.56570796793, 9.26567565083, 10.0189288068, 10.8075612845, 11.546746107, 12.2678551476, 13.0573814653, 13.8537984165, 14.840420662, 22.7578170253, 30.4276253614, 37.9495715235, 45.9357240423, 53.6216104001, 61.206661593, 69.4889438286, 77.3913187831],
            #[__2pl.att_latency_99__],
            #[__2pl_wait_die.att_latency_99__],
            [1.82111349666, 2.22362067533, 2.88726941854, 3.70289725894, 4.55179347046, 5.48274762065, 6.42631422731, 7.3534702077, 8.31750227434, 9.31571779002, 10.2893346122, 11.372899179, 12.3169187457, 13.32734702, 14.38054705, 15.335038928, 16.4129405739, 17.4837989596, 18.4477520337, 19.6042555073, 28.9256029501, 37.2145536153, 44.1455406218, 50.9061082, 57.6212074976, 64.2855828321, 71.3284272284, 78.1958512244]
            ]

    att_latency_999_val = [
            [1.70741974979, 1.98150221759, 2.37788835681, 2.92070508174, 3.59630063725, 4.29040391216, 5.05951143731, 5.84250180365, 6.647555907, 7.4483749936, 8.32759829586, 9.18117688369, 10.0163357405, 10.9197317349, 11.7960215915, 12.698061155, 13.6619458813, 14.6854844814, 15.7205848406, 16.7021765275, 27.0135499744, 36.5663299144, 47.4156268326, 57.9624214467, 56.8002814281, 72.2927010293, 73.9545608629, 106.747962207],
            [1.48782970997, 1.79065168034, 2.25282067546, 2.86841677978, 3.55073792923, 4.29199448296, 5.02375231684, 5.75282528854, 6.50413391922, 7.21845297764, 7.91217606029, 8.62985143145, 9.40032678195, 10.1542788567, 10.9687223588, 11.7113859384, 12.3950514568, 13.2077564839, 14.0674307908, 14.974989077, 22.9837161577, 30.7306332944, 38.3524608246, 46.4158757857, 54.0380670429, 61.5720631977, 69.9010919193, 77.7803050803],
            #[__2pl.att_latency_999__],
            #[__2pl_wait_die.att_latency_999__],
            [1.82654905539, 2.23081791702, 2.89664280287, 3.71433692685, 4.56604345313, 5.50026896182, 6.44754958375, 7.37825662366, 8.3466836231, 9.348426529, 10.3523336836, 11.4426005504, 12.389360694, 13.4030130455, 14.4587448724, 15.4133477032, 16.4950924503, 17.5697675982, 18.5358807544, 19.6954221616, 29.1098251995, 37.5760326926, 44.8096701185, 51.9697177143, 59.0634424582, 66.2224650952, 73.7230205928, 81.0230080344]
            ]

    latency_min_val = [
            [1.09008789062, 1.17626953125, 1.38525390625, 1.53857421875, 1.27490234375, 1.1474609375, 1.73364257812, 2.00952148438, 1.86669921875, 1.7216796875, 2.57934570312, 2.31225585938, 1.84643554688, 1.99633789062, 1.720703125, 1.59985351562, 1.57299804688, 1.44897460938, 1.44482421875, 1.40454101562, 1.29565429688, 1.30493164062, 1.28881835938, 1.27783203125, 35.2578125, 1.2724609375, 49.3005371094, 1.171875],
            [0.866455078125, 0.996337890625, 1.0048828125, 1.015625, 0.842041015625, 1.03369140625, 0.97314453125, 1.00415039062, 0.9970703125, 1.12182617188, 1.08666992188, 1.14916992188, 1.14672851562, 1.01953125, 1.0966796875, 1.01733398438, 1.12890625, 1.0068359375, 1.01245117188, 0.965576171875, 0.90283203125, 1.10107421875, 0.996826171875, 1.28393554688, 1.15698242188, 1.16967773438, 1.40991210938, 1.27221679688],
            #[__2pl.min_latency__],
            #[__2pl_wait_die.min_latency__],
            [1.12866210938, 1.25439453125, 1.43920898438, 1.43334960938, 1.78442382812, 1.72607421875, 1.72387695312, 2.13793945312, 2.12280273438, 2.4296875, 2.57177734375, 3.28955078125, 3.58740234375, 3.57446289062, 4.30004882812, 4.14868164062, 4.00561523438, 5.87451171875, 6.41088867188, 7.30493164062, 9.77490234375, 10.7414550781, 12.5913085938, 14.0341796875, 24.0751953125, 31.5617675781, 36.5432128906, 46.51953125]
            ]

    latency_max_val = [
            [5.74658203125, 6.87915039062, 9.89086914062, 13.8933105469, 22.3566894531, 32.3706054688, 32.5256347656, 31.1000976562, 54.7602539062, 41.4172363281, 543.739501953, 759.59765625, 1504.63793945, 802.071533203, 1439.90478516, 1571.48095703, 2361.74316406, 3090.74951172, 3350.01953125, 3528.10717773, 19143.7487793, 27155.4645996, 24413.3034668, 40812.3798828, 9316.87963867, 14955.3974609, 17278.2351074, 34112.9438477],
            [31.9965820312, 68.4677734375, 70.2331542969, 136.350341797, 135.045410156, 133.596923828, 133.041503906, 134.068115234, 73.2507324219, 77.4006347656, 141.041748047, 201.978027344, 143.772705078, 145.931640625, 150.657470703, 85.5812988281, 146.725830078, 153.375732422, 151.944335938, 150.651855469, 158.976074219, 179.901367188, 177.611328125, 195.522460938, 199.681152344, 236.514404297, 254.250488281, 267.346923828],
            #[__2pl.max_latency__],
            #[__2pl_wait_die.max_latency__],
            [5.72729492188, 7.41821289062, 9.67358398438, 12.4677734375, 19.1979980469, 21.2155761719, 25.5261230469, 34.3986816406, 35.466796875, 49.7407226562, 44.4243164062, 51.3132324219, 58.7937011719, 61.7802734375, 67.6560058594, 70.2370605469, 91.0227050781, 84.4272460938, 104.929443359, 128.291992188, 148.654052734, 294.888671875, 401.238769531, 535.128173828, 684.944091797, 913.280273438, 1109.15283203, 1251.74707031]
            ]

    attempts_val = [
            [123641.0/26.4890850131, 180786.0/22.4657487022, 235577.0/23.4038241352, 261628.0/23.9329032288, 269980.0/24.3741423443, 270816.0/24.2634171944, 265886.0/24.0566528766, 262801.0/24.0207210444, 259253.0/23.9633064491, 257434.0/23.9946873204, 229541.0/21.7419846548, 225783.0/21.6121618526, 223607.0/21.5541562866, 220108.0/21.4776907214, 217509.0/21.3994775941, 215245.0/21.3711389395, 212510.0/21.3638627769, 208771.0/21.3060587062, 205627.0/21.2816675916, 216380.0/22.602721141, 198715.0/22.3806818886, 196007.0/22.4098746025, 189976.0/22.5311604753, 188174.0/22.7348190738, 229971.0/23.3324232174, 206939.0/23.3963753234, 230616.0/23.6927022851, 178204.0/23.7928065627],
            [113110.0/21.3556829474, 196261.0/22.55309552, 242724.0/23.3674340453, 257332.0/23.639551677, 262439.0/23.7822877232, 261052.0/23.7547648961, 262179.0/23.8775784749, 261460.0/23.7982389208, 261279.0/23.8537408615, 262544.0/23.9193136885, 242131.0/21.9753196692, 242166.0/21.9953575829, 241262.0/22.0051626592, 241045.0/22.0718611968, 238133.0/21.9577274545, 238939.0/22.00001011, 240240.0/22.0449596898, 239389.0/22.1395928678, 236840.0/22.0840204372, 255589.0/24.080125813, 252043.0/24.2501209207, 255073.0/24.6003474339, 195158.0/18.7731913256, 196153.0/19.0219315857, 199354.0/19.2780360484, 268890.0/25.919945349, 268476.0/26.1149462502, 271959.0/26.4908510019],
            #[__2pl.attempts__],
            #[__2pl_wait_die.attempts__],
            [115215.0/26.3968919697, 157975.0/22.0940080211, 186669.0/22.583491255, 195923.0/22.7861880841, 200036.0/22.87389013, 200009.0/22.9549266383, 198942.0/22.9377917845, 198916.0/22.9624175058, 197590.0/22.9346258635, 196324.0/22.9697903957, 183319.0/21.5958046062, 180623.0/21.5579439874, 180486.0/21.5291166475, 179821.0/21.5467125613, 178784.0/21.5664268398, 178702.0/21.5434239436, 177264.0/21.5250374875, 176274.0/21.5307319686, 176341.0/21.5281108906, 186037.0/22.9232560445, 189602.0/23.0231047332, 196185.0/23.0709503624, 205986.0/23.1242394881, 214220.0/23.2576836909, 221474.0/23.4363342754, 226638.0/23.5512245763, 230824.0/23.745385842, 235856.0/24.0120397614]
            ]

    graph_size_val = [
            [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
            ]

    n_ask_val = [
            [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
            ]

    scc_val = [
            [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
            ]

    throughput_figname =            "fig/kodiak/tpcc_no_10_nlog_ct_tp.pdf"
    commit_rate_figname =           "fig/kodiak/tpcc_no_10_nlog_ct_cr.pdf"
    latency_50_figname =            "fig/kodiak/tpcc_no_10_nlog_ct_lt_50.pdf"
    latency_90_figname =            "fig/kodiak/tpcc_no_10_nlog_ct_lt_90.pdf"
    latency_99_figname =            "fig/kodiak/tpcc_no_10_nlog_ct_lt_99.pdf"
    latency_999_figname =           "fig/kodiak/tpcc_no_10_nlog_ct_lt_999.pdf"
    n_try_50_figname =              "fig/kodiak/tpcc_no_10_nlog_ct_nt_50.pdf"
    n_try_90_figname =              "fig/kodiak/tpcc_no_10_nlog_ct_nt_90.pdf"
    n_try_99_figname =              "fig/kodiak/tpcc_no_10_nlog_ct_nt_99.pdf"
    n_try_999_figname =             "fig/kodiak/tpcc_no_10_nlog_ct_nt_999.pdf"
    attempts_figname =              "fig/kodiak/tpcc_no_10_nlog_ct_at.pdf"
    tpcc_ct_lt_bar_figname =        "fig/kodiak/tpcc_no_10_nlog_ct_lt_bar.pdf"
    tpcc_ct_lt_eb_figname =         "fig/kodiak/tpcc_no_10_nlog_ct_lt_eb.pdf"
    tpcc_ct_att_lt_bar_figname =    "fig/kodiak/tpcc_no_10_nlog_ct_att_lt_bar.pdf"
    tpcc_ct_att_lt_eb_figname =     "fig/kodiak/tpcc_no_10_nlog_ct_att_lt_eb.pdf"
    tpcc_ct_nt_eb_figname =         "fig/kodiak/tpcc_no_10_nlog_ct_nt_eb.pdf"
    tpcc_ct_cpu_figname =           "fig/kodiak/tpcc_no_10_nlog_ct_cpu.pdf"

    figure.tpcc_ct_tp(throughput_val, throughput_figname);
    figure.tpcc_ct_cr(commit_rate_val, commit_rate_figname);
    figure.tpcc_ct_lt(latency_50_val, latency_50_figname);
    figure.tpcc_ct_lt(latency_90_val, latency_90_figname);
    figure.tpcc_ct_lt(latency_99_val, latency_99_figname);
    figure.tpcc_ct_lt(latency_999_val, latency_999_figname);
    figure.tpcc_ct_at(attempts_val, attempts_figname);
    figure.tpcc_ct_cpu(cpu, tpcc_ct_cpu_figname);
#    figure.tpcc_ct_lt_eb(latency_50_val, latency_90_val, latency_99_val, tpcc_ct_lt_eb_figname)
    #figure.tpcc_ct_lt_eb(att_latency_50_val, att_latency_90_val, att_latency_99_val, tpcc_ct_att_lt_eb_figname)
#    figure.tpcc_ct_lt_eb(n_try_50_val, n_try_90_val, n_try_99_val, tpcc_ct_nt_eb_figname)

    figure.tpcc_ct_nt(n_try_50_val, n_try_50_figname);
    figure.tpcc_ct_nt(n_try_90_val, n_try_90_figname);
    figure.tpcc_ct_nt(n_try_99_val, n_try_99_figname);
    figure.tpcc_ct_nt(n_try_999_val, n_try_999_figname);
#    print("Hello")
